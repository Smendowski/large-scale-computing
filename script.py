#!/usr/bin/env python3
import requests
import os
import time
import base64
import shutil
import paramiko
from dotenv import load_dotenv


ARES_HOST_URL = "ares.cyfronet.pl"
RIMROCK_JOBS_URL = 'https://submit.plgrid.pl/api/jobs'
DATA_PLGRID_URL = 'https://data.plgrid.pl'


def connect_to_ares() -> paramiko.client.SSHClient:
    ctx = paramiko.SSHClient()
    ctx.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ctx.connect(
        ARES_HOST_URL,
        username=os.getenv('USERNAME'),
        password=os.getenv('PASSWORD')
    )
    return ctx


def issue_command(ctx: paramiko.client.SSHClient, command: str) -> str:
    stdin, stdout, stderr = ctx.exec_command(command)
    stdin.close()
    if not stderr.readlines():
        response = stdout.readlines()
        return response
    return stderr.readlines()


def get_file_content(ctx: paramiko.client.SSHClient, path: str) -> str:
    return issue_command(ctx, f"cat {path}")


def get_grid_proxy_cert_path(ctx: paramiko.client.SSHClient) -> str:
    def _check_validity(response: str) -> str | None:
        grid_proxy_info = {}
        for pair in response:
            k, v = pair.split(r' : ')
            grid_proxy_info[k.rstrip()] = v.rstrip()
        if grid_proxy_info['timeleft'] != '00:00:00':
            return grid_proxy_info['path']
        return None

    response = issue_command(ctx, "grid-proxy-info")
    path = _check_validity(response)
    if not path:
        response = issue_command(ctx, "grid-proxy-init")
        path = _check_validity(response)
    return path


def get_proxy_cert_encoded(content: str) -> str:
    base64_bytes = base64.b64encode("".join(content).encode('ascii'))
    return base64_bytes.decode('utf-8').replace('\n', '')


def format_planet_script(start_frame: int, end_frame: int) -> str:
    return "#!/bin/bash\n#SBATCH -p plgrid\n#SBATCH --time=02:00\n" \
           "module add pov-ray\n" \
           f"povray Subset_Start_Frame={start_frame} Subset_End_Frame={end_frame} planet_00ani.ini"   # noqa: E501


def get_image_names(start_frame: int, end_frame: int):
    png_names = []
    for id in range(start_frame, end_frame+1):
        png_names.append(f"planet_00ani{str(id).zfill(3)}.png")
    return png_names


def download_file_from_plgrid(filename: str, headers) -> None:
    url = DATA_PLGRID_URL + '/download/ares/net/people/plgrid/' + os.getenv("USERNAME") + f'/{filename}'  # noqa: E501
    with requests.get(url, headers=headers, stream=True) as r:
        if r.status_code == 200:
            with open(f'./{filename}', 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)


def submit(n, headers, json_data):
    for i in range(n):
        iteration = i + 1
        end_frame = iteration * 2
        start_frame = end_frame - 1

        print(f"Starting iteration {iteration}.")
        print(f"Frames range to render: {start_frame}-{end_frame}")

        json_data['script'] = format_planet_script(start_frame, end_frame)
        post_response = requests.post(
            RIMROCK_JOBS_URL, headers=headers, json=json_data)

        if post_response.status_code == 201:
            job_id = post_response.json()['job_id']
            while True:
                get_response = requests.get(
                    f"{RIMROCK_JOBS_URL}/{job_id}",
                    headers=headers
                )
                content = get_response.json()
                print(content['status'])
                status_code = get_response.status_code

                if status_code in [401, 404, 500]:
                    break
                elif status_code == 200 and content['status'] == "FINISHED":
                    filenames = get_image_names(start_frame, end_frame)
                    for filename in filenames:
                        download_file_from_plgrid(filename, headers)
                        time.sleep(5)
                        print(f"Downloaded: {filename}")
                    print(f"Finished iteration {iteration}.")
                    break
                time.sleep(5)


def main() -> None:
    load_dotenv()
    ctx = connect_to_ares()
    cert_path = get_grid_proxy_cert_path(ctx)
    cert_content = get_file_content(ctx, cert_path)
    proxy = get_proxy_cert_encoded(cert_content)

    headers = {
        'Content-Type': 'application/json',
        'PROXY': f"{proxy}",
    }

    json_data = {
        'host': 'ares.cyfronet.pl',
        'working_directory': f"/net/people/plgrid/{os.getenv('USERNAME')}",
    }

    submit(10, headers, json_data)


if __name__ == "__main__":
    main()
