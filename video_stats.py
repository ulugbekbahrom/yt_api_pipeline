import os, json, requests
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"

def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle=MrBeast&key={API_KEY}"

        response = requests.get(url)
        print(response)

        data = response.json()
        # print(json.dumps(data, indent=4))

        channel_items = data["items"][0]
        channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        print(channel_playlist_id)
        return channel_playlist_id

    except requests.exceptions.RequestException as ex:
        raise ex
    
if __name__ == "__main__":
    get_playlist_id()