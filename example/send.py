import asyncio
import aiohttp
from pydantic import BaseModel, ValidationError


class Message(BaseModel):
    car_count: int
    bike_count: int
    bus_count: int
    truck_count: int
    total: int
    traffic_situation: str


async def send_sensor_data(file_path: str, base_url: str, environment_id: str, device_id: str):
    url = f"{base_url}/data/http/{environment_id}/{device_id}"

    async with aiohttp.ClientSession() as session:
        with open(file_path, "r") as file:
            # Skip header
            next(file)
            for index, line in enumerate(file, start=1):
                parts = line.strip().split(",")

                try:
                    message = Message(
                        car_count=int(parts[0]),
                        bike_count=int(parts[1]),
                        bus_count=int(parts[2]),
                        truck_count=int(parts[3]),
                        total=int(parts[4]),
                        traffic_situation=parts[5],
                    )

                    json_data = message.json()
                    headers = {"Content-Type": "application/json"}

                    # 发送当前数据
                    await post_data(session, url, json_data, headers, index, file_path)

                    # 每发送一条数据暂停3秒
                    await asyncio.sleep(3)

                except ValidationError as e:
                    print(f"Validation error for Data {index} from {file_path}: {e}")


async def post_data(session, url, json_data, headers, index, file_path):
    try:
        async with session.post(url, data=json_data, headers=headers) as response:
            if response.status == 200:
                print(f"Data {index} sent successfully from {file_path}")
            else:
                text = await response.text()
                print(
                    f"Failed to send Data {index} from {file_path}. "
                    f"Status code: {response.status}, Response: {text}"
                )
    except aiohttp.ClientError as e:
        print(f"Failed to send Data {index} from {file_path}: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 5:
        print("Usage: python3 send.py <file_path> <base_url> <environment_id> <device_id>")
        sys.exit(1)

    file_path = sys.argv[1]
    base_url = sys.argv[2].rstrip("/")  # 去掉末尾的 /
    environment_id = sys.argv[3]
    device_id = sys.argv[4]

    asyncio.run(send_sensor_data(file_path, base_url, environment_id, device_id))

#python send.py ./data/sensor_road1.csv http://0.0.0.0:5000 env-smart-city sensor1
#python send.py ./data/sensor_road2.csv http://0.0.0.0:5000 env-smart-city sensor2
#python send.py ./data/sensor_road3.csv http://0.0.0.0:5000 env-smart-city sensor3