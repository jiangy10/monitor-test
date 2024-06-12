import pytest
import asyncio
import websockets
from azure.core.exceptions import HttpResponseError, ServiceRequestError
from azure.messaging.webpubsubservice import WebPubSubServiceClient
from devtools_testutils import recorded_by_proxy
from testcase import WebpubsubPowerShellPreparer, WebpubsubTest

class SimpleWebSocketFrame:
    def __init__(self, data, is_binary):
        self.is_binary = is_binary
        if not is_binary:
            self.data_as_string = data.decode('utf-8')
        else:
            self.data_as_string = data

    def is_end_signal(self, id=None):
        if self.is_binary and len(self.data_as_string) >= 4:
            return self.data_as_string[0] == 5 and self.data_as_string[1] == 1 and self.data_as_string[2] == 1 and (id is None or self.data_as_string[3] == id)
        return False

    def __str__(self):
        if self.is_end_signal():
            return "|EndSignal|"
        return self.data_as_string if not self.is_binary else ''

class PubSubWebSocketFrame:
    def __init__(self, data, is_binary):
        assert not is_binary, "Expected non-binary data"
        self.data_as_string = data.decode('utf-8')
        self.message = json.loads(self.data_as_string)

    def is_end_signal(self):
        return self.message.get('dataType') == 'binary' and self.message.get('data') == 'BQEB'

    def __str__(self):
        if self.is_end_signal():
            return "|EndSignal|"
        return self.data_as_string

def get_end_signal(id=None):
    payload = bytearray([5, 1, 1])
    if id is not None:
        payload.append(id)
    return bytes(payload)

@pytest.fixture
async def service_client():
    connection_string = "WPS_CONNECTION_STRING"
    hub = "SimpleClientCanReceiveMessage"
    return WebPubSubServiceClient.from_connection_string(connection_string, hub)

@pytest.mark.asyncio
async def test_simple_clients_can_receive_expected_messages_with_different_content_types(service_client):
    if not is_live_mode():
        pytest.skip("Skipping live test")
    hub = "SimpleClientCanReceiveMessage"
    messages = []

    token = await service_client.get_client_access_token()
    end_signal = asyncio.Future()

    async with websockets.connect(token['url']) as websocket:
        async def receive_messages():
            async for message in websocket:
                frame = SimpleWebSocketFrame(message, False)
                print(frame)
                if frame.is_end_signal():
                    end_signal.set_result(None)
                    await websocket.close()
                else:
                    messages.append(frame)

        receive_task = asyncio.create_task(receive_messages())

        await websocket.send(json.dumps({"message": "Hello world!"}))
        await websocket.send("Hi there!")
        await websocket.send(get_end_signal())

        await end_signal
        await receive_task

    assert len(messages) == 2
    assert messages[0].data_as_string == '{"message":"Hello world!"}'
    assert messages[1].data_as_string == "Hi there!"

@pytest.mark.asyncio
async def test_simple_clients_can_join_group_and_receive_group_messages(service_client):
    if not is_live_mode():
        pytest.skip("Skipping live test")
    hub = "SimpleClientCanReceiveGroupMessage"
    messages = []

    token = await service_client.get_client_access_token(groups=["group1", "group2"])
    end1_signal = asyncio.Future()
    end2_signal = asyncio.Future()

    async with websockets.connect(token['url']) as websocket:
        async def receive_messages():
            async for message in websocket:
                frame = SimpleWebSocketFrame(message, False)
                print(frame)
                if frame.is_end_signal(1):
                    end1_signal.set_result(None)
                elif frame.is_end_signal(2):
                    end2_signal.set_result(None)
                else:
                    messages.append(frame)

        receive_task = asyncio.create_task(receive_messages())

        group1_client = service_client.group("group1")
        group2_client = service_client.group("group2")

        await group1_client.send_to_all({"message": "Hello world from group1!"})
        await group2_client.send_to_all(get_end_signal(1))
        await group2_client.send_to_all("Hi there from group2!", content_type="text/plain")
        await group2_client.send_to_all(get_end_signal(2))

        await end1_signal
        await end2_signal
        await receive_task

    assert len(messages) == 2
    assert any(msg.data_as_string == '{"message":"Hello world from group1!"}' for msg in messages)
    assert any(msg.data_as_string == "Hi there from group2!" for msg in messages)

@pytest.mark.asyncio
async def test_subprotocol_clients_can_receive_expected_messages_with_different_content_types(service_client):
    if not is_live_mode():
        pytest.skip("Skipping live test")
    hub = "PubSubClientCanReceiveMessage"
    messages = []

    token = await service_client.get_client_access_token()
    end_signal = asyncio.Future()
    connected_signal = asyncio.Future()

    async with websockets.connect(token['url'], subprotocols=["json.webpubsub.azure.v1"]) as websocket:
        async def receive_messages():
            async for message in websocket:
                frame = PubSubWebSocketFrame(message, False)
                print(frame)
                if frame.message.get("event") == "connected":
                    connected_signal.set_result(None)
                if frame.is_end_signal():
                    end_signal.set_result(None)
                    await websocket.close()
                else:
                    messages.append(frame)

        receive_task = asyncio.create_task(receive_messages())

        await connected_signal

        await service_client.send_to_all({"message": "Hello world!"})
        await service_client.send_to_all("Hi there!", content_type="text/plain")
        await service_client.send_to_all(get_end_signal())

        await end_signal
        await receive_task

    assert len(messages) == 3
    assert messages[0].message.get("event") == "connected"
    assert messages[1].data_as_string == '{"type":"message","from":"server","dataType":"json","data":{"message":"Hello world!"}}'
    assert messages[2].data_as_string == '{"type":"message","from":"server","dataType":"text","data":"Hi there!"}'

@pytest.mark.asyncio
async def test_clients_can_receive_messages_with_filters(service_client):
    if not is_live_mode():
        pytest.skip("Skipping live test")
    hub = "ClientCanReceiveMessageWithFilters"
    messages = []

    token = await service_client.get_client_access_token(groups=["groupA"])
    end_signal = asyncio.Future()

    async with websockets.connect(token['url']) as websocket:
        async def receive_messages():
            async for message in websocket:
                frame = SimpleWebSocketFrame(message, False)
                print(frame)
                if frame.is_end_signal():
                    end_signal.set_result(None)
                    await websocket.close()
                else:
                    messages.append(frame)

        receive_task = asyncio.create_task(receive_messages())

        await service_client.send_to_all({"message": "Hello world!"}, filter="userId eq null", message_ttl_seconds=60)
        await service_client.send_to_all("Hello world!", content_type="text/plain", filter=odata("groupA in groups and not(groupB in groups)"), message_ttl_seconds=60)
        await service_client.send_to_all(get_end_signal())

        await end_signal
        await receive_task

    assert len(messages) == 2
    assert messages[0].data_as_string == '{"message":"Hello world!"}'
    assert messages[1].data_as_string == "Hello world!"

@pytest.mark.asyncio
async def test_clients_can_join_or_leave_multiple_groups_with_filter(service_client):
    if not is_live_mode():
        pytest.skip("Skipping live test")
    hub = "ClientsCanJoinOrLeaveMultipleGroupsWithFilter"
    messages = []

    token = await service_client.get_client_access_token(user_id="user 1")
    start_signal = asyncio.Future()
    end_signal = asyncio.Future()

    async with websockets.connect(token['url']) as websocket1, websockets.connect(token['url']) as websocket2:
        async def receive_messages(websocket):
            async for message in websocket:
                frame = SimpleWebSocketFrame(message, False)
                print(frame)
                messages.append(frame)
                if len(messages) == 6:
                    end_signal.set_result(None)

        receive_task1 = asyncio.create_task(receive_messages(websocket1))
        receive_task2 = asyncio.create_task(receive_messages(websocket2))

        await start_signal

        await service_client.add_connections_to_groups(["group1", "group2"], "userId eq 'user 1'")
        await service_client.send_to_all({"message": "Hi json!"}, filter="'group1 1' in groups and 'group2' in groups")
        await service_client.send_to_all("Hi text!", content_type="text/plain", filter="'group1 1' in groups and 'group2' in groups")
        await service_client.send_to_all(get_end_signal(2), filter="'group1 1' in groups and 'group2' in groups")

        await asyncio.wait_for(end_signal, timeout=1)
        await receive_task1
        await receive_task2

    assert len(messages) == 6
    websocket1.close()
    websocket2.close()
