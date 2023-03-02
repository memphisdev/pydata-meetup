import asyncio
import json
import uuid
import requests
import boto3
import consts

from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError

memphis = Memphis()

class MessageHandler():
    def create_and_put_bucket(self, bucket_name, msg_type, msgs):
        try:
            client = boto3.client('s3', aws_access_key_id=consts.AWS_ACCESS_KEY, aws_secret_access_key=consts.AWS_SECRET_KEY)
            head_bucket_response = client.head_bucket(Bucket=bucket_name)
            if head_bucket_response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception("create head bucket failure")

        except Exception as e:
            if e.response['Error']['Code'] == '404' and e.response['Error']['Message'] == 'Not Found':
                create_bucket_response = client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': 'eu-central-1'
                    },
                )
                if create_bucket_response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise Exception("creation bucket failure")
                response_public = client.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                    },
                )
                if response_public['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise Exception("creation private access for bucket failure")  
        finally:
            try:
                uid = uuid.uuid1()  
                response_put_object = client.put_object(Body=json.dumps(msgs), Bucket=bucket_name, Key= msg_type+"/"+str(uid)+".json")
                if response_put_object['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise Exception("put object to bucket failure")
            except Exception as e:
                return e

    async def msg_handler_raw_to_transformed(self, msgs, error):
        try:
            if len(msgs) > 0 :
                dict_msgs = []
                raw_msgs = msgs[0]
                message = raw_msgs.get_data()
                for msg in msgs:
                    await msg.ack()
            
                dict_msg = json.loads(message.decode('utf-8'))
                for m in dict_msg['results']:
                    m['person']['is_identified'] = str(
                        m['person']['is_identified']).lower()
                    dict_msgs.append(m)
    
                lambda_response = requests.post(url=consts.TRANSFORMED_LAMBDA_URL,
                                json={
                                    "payload": dict_msgs,
                                }, headers={"content-type": "application/json"}
                                )
                if lambda_response.status_code != 200:
                    raise Exception("msg_handler_raw_to_transformed: lambda error")
                if error:
                    return error
        except (MemphisError, MemphisConnectError, MemphisHeaderError, Exception) as e:
            return e

    async def msg_handler_transformed_to_enrich(self,msgs, error):
        try: 
            dict_msgs = []
            if len(msgs) > 0:
                for msg in msgs:
                    message = msg.get_data()
                    await msg.ack()
                    dict_msg = json.loads(message.decode('utf-8')) 
                    dict_msgs.append(dict_msg)
            
                if len(dict_msgs) > 0:
                    lambda_response = requests.post(url=consts.ENRICHED_LAMBDA_URL,
                                    json={
                                        "payload": dict_msgs
                                    }, headers={"content-type": "application/json"}
                                    )
                    if lambda_response.status_code != 200:
                        raise Exception("msg_handler_transformed_to_enrich: lambda error")
            if error:
                raise Exception("msg_handler_transformed_to_enrich: "+ error)
        except (MemphisError, MemphisConnectError, MemphisHeaderError, Exception) as e:
            return e
    
    async def msg_handler_upload_to_s3(self, msgs, error):
        try:
            faq_msgs, main_page_msgs, blog_msgs = [], [], []
            if len(msgs) > 0 :
                for msg in msgs:
                    message = msg.get_data()
                    await msg.ack()
                    dict_msg = json.loads(message.decode('utf-8'))

                    if 'type' in dict_msg:
                        if dict_msg['type'] == "blog":
                            blog_msgs.append(dict_msg)
                        elif dict_msg['type'] == "faq":
                            faq_msgs.append(dict_msg)
                        elif dict_msg['type'] == "main-page":
                            main_page_msgs.append(dict_msg)
            if error:
                raise Exception("msg_handler_upload_to_s3:" + error)
        except Exception as e:
            return e
        finally:
            if len(main_page_msgs) > 0 :
                self.create_and_put_bucket("py-data-meetup","main_page", main_page_msgs)
            if len(blog_msgs) > 0 :
                self.create_and_put_bucket("py-data-meetup","blog", blog_msgs)
            if len(faq_msgs) > 0 :
                self.create_and_put_bucket("py-data-meetup","faq", faq_msgs)

def get_raw_data():
    try:
        get_posthog_events = "https://app.posthog.com/api/projects/12503/events/?personal_api_key="+consts.API_KEY_POSTHOG
        res = requests.get(get_posthog_events)
        if res.status_code != 200:
            raise Exception("posthog error")
        data = res.json()
        return data
    except Exception as e:
        return e

async def raw_step():
    try:
        print("raw service start")
        producer = await memphis.producer(station_name="raw_data", producer_name="raw_data_producer")

        while True:
            data = get_raw_data()
            if len(data) > 0:
                raw_data = json.dumps(data).encode('utf-8')
                await producer.produce(bytearray(raw_data))
    except Exception as e:
        return e

async def transformed_step():
    try:
        print("transform service start")
        consumer = await memphis.consumer(station_name="raw_data", consumer_name="posthog_consumer", consumer_group="", batch_size=1000)
        message_handler = MessageHandler()
        consumer.consume(message_handler.msg_handler_raw_to_transformed)
    except Exception as e:
        return e

async def enriched_step():
    try:
        print("enrich service start")
        message_handler = MessageHandler()
        consumer = await memphis.consumer(station_name="transformed_data", consumer_name="tranformed_consumer", consumer_group="", batch_size=1000)
        consumer.consume(message_handler.msg_handler_transformed_to_enrich)
    except (MemphisError, MemphisConnectError, MemphisHeaderError, Exception) as e:
        return e

async def save_to_s3_bucket():
    try:
        consumer = await memphis.consumer(station_name="enriched_data", consumer_name="enriched_consumer", consumer_group="", batch_size=1000)
        message_handler = MessageHandler()
        consumer.consume(message_handler.msg_handler_upload_to_s3)
    except (MemphisError, MemphisConnectError, MemphisHeaderError, Exception) as e:
        return e

async def main():
    try:
        await memphis.connect(host=consts.HOST, username=consts.USER_NAME, connection_token=consts.CONNECTION_TOKEN)
        asyncio.create_task(raw_step())
        asyncio.create_task(transformed_step())
        asyncio.create_task(enriched_step())
        asyncio.create_task(save_to_s3_bucket())
        await asyncio.Event().wait()
    except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
        return e
    finally:
        await memphis.close()
        return

if __name__ == '__main__':
    asyncio.run(main())
