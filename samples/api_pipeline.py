import os
import sys

currentdir = f"{str(os.getcwd())}\\td-ameritrade-python-api\\samples"
parentdir = os.path.dirname(currentdir)
if currentdir not in sys.path:
    sys.path.append(currentdir)
if parentdir not in sys.path:
    sys.path.append(parentdir)
if f"{parentdir}\\td" not in sys.path:
    sys.path.append(f"{parentdir}\\td")

import asyncio
import pprint
from td.client import TDClient
from configparser import ConfigParser

config = ConfigParser()
config.read('config/config.ini')

# Create a new session
TDSession = TDClient(
    client_id=config.get('main', 'CLIENT_ID'),
    redirect_uri=config.get('main', 'REDIRECT_URI'),
    credentials_path=config.get('main', 'JSON_PATH')
)

# Login to the session
TDSession.login()

# Create a streaming session
TDStreamingClient = TDSession.create_streaming_session()
TDStreamingClient.quality_of_service(qos_level="express")

# Set the data dump location
TDStreamingClient.write_behavior(
    file_path = "raw_data.csv", 
    append_mode = True
)

"""Below are the individual data requests getting streamed from the websocket"""

# Level One Quote
TDStreamingClient.level_one_quotes(
    symbols=["SPY"],
    fields=list(range(0, 50))
)

# # Level One Forex
# TDStreamingClient.level_one_forex(
#     symbols=['EUR/USD'], 
#     fields=list(range(0,26))
# )

# # Level One Option
# TDStreamingClient.level_one_futures(
#     symbols=['/ES'],
#     fields=list(range(0, 42))
# )


# Data Pipeline function

async def data_pipeline():
    """
    This is an example of how to build a data pipeline,
    using the library. A common scenario that would warrant
    using a pipeline is taking data that is sent back to the stream
    and processing it so it can be used in other programs or functions.

    Generally speaking, you will need to wrap the operations that process
    and handle the data inside an async function. The reason being is so
    that you can await the return of data.

    However, operations like creating the client, and subscribing to services
    can be performed outside the async function. In the example below, we demonstrate
    building the pipline, which is connecting to the websocket and logging in.

    We then start the pipeline, which is where the services are subscribed to and data
    begins streaming. The `start_pipeline()` will return the data as it comes in. From
    there, we process the data however we choose.

    Additionally, we can also see how to unsubscribe from a stream using logic and how
    to close the socket mid-stream.
    """

    data_response_count = 0
    heartbeat_response_count = 0

    # Build the Pipeline.
    await TDStreamingClient.build_pipeline()

    """
    Wrap any functionality/processing you want applied to all of the stream
    data inside of the below while loop
    """
    
    # Keep going as long as we can recieve data.
    while True:

        # Start the Pipeline.
        data = await TDStreamingClient.start_pipeline()

        # Grab the Data, if there was any. Remember not every message will have `data.`
        if 'data' in data:

            print('='*80)
            data_content = data['data'][0]['content']
            pprint.pprint(data_content, indent=4)

            # Here I can grab data as it comes in and do something with it.
            if 'key' in data_content[0]:
                print('Here is my key: {}'.format(data_content[0]['key']))

            print('-'*80)
            data_response_count += 1

        # If we get a heartbeat notice, let's increment our counter.
        elif 'notify' in data:
            print(data['notify'][0])
            heartbeat_response_count += 1

        # Once we have 1 data responses, we can unsubscribe from a service.
        if data_response_count == 1:
            unsub = await TDStreamingClient.unsubscribe(service='QUOTES')
            data_response_count += 1
            print('='*80)
            print(unsub)
            print('-'*80)

        # Stream automatically ends after 100 data responses are received
        elif data_response_count == 100:
            await TDStreamingClient.close_stream()
            break

        # Once we have 3 heartbeats, let's close the stream. Make sure to break the while loop.
        # or else you will encounter an exception.
        if heartbeat_response_count == 3:
            await TDStreamingClient.close_stream()
            break

# Run the pipeline.
asyncio.run(data_pipeline())
