import boto3
import time
import requests
import paramiko
from pprint import pprint
from typing import Tuple
from botocore.exceptions import ClientError


class EC2InstanceNotFoundError(Exception):
    """Exception is raised when EC2 instance is not found."""


class EC2InstanceStartError(Exception):
    """Exception is raised when EC2 cannot be started."""


class EC2InstanceStopError(Exception):
    """Exception is raised when EC2 cannot be stopped."""


class EC2InstanceStateError(Exception):
    """Exception is raised when EC2 has incorrect state."""


class ClientEC2:
    def __init__(self, session: boto3.session.Session) -> None:
        self.ec2_session = session.resource('ec2', 'us-east-1')
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')

    @property
    def instances(self):
        return self.list_instances()

    def list_instances(self):
        instances = {}
        for instance in self.ec2_session.instances.filter():
            if instance:
                instances[instance.id] = instance

        return instances

    def _check_instance_exists(self, instance_id: str) -> bool:
        return instance_id in self.instances

    def _get_instance_state(self, instance_id: str) -> str:
        return self.instances[instance_id].state['Name']

    def _start_instance(self, instance_id: str) -> None:
        try:
            start = time.perf_counter()
            self.instances[instance_id].start()
            while self._get_instance_state(instance_id) != "running":
                print(self._get_instance_state(instance_id))
                time.sleep(1)
            stop = time.perf_counter()
            print(f"Took about {stop-start:.2f} seconds to start instance.")

        except ClientError as e:
            raise EC2InstanceStartError(
                f"Cannot start instance {instance_id}. Reason: {str(e)}")

    def start_instance_by_id(self, instance_id: str) -> None:
        if not self._check_instance_exists(instance_id):
            raise EC2InstanceNotFoundError(
                f"Instance {instance_id} not found.")

        if (state := self._get_instance_state(instance_id)) != "stopped":
            raise EC2InstanceStateError(
                f"Cannot start {state} instance.")

        self._start_instance(instance_id)

    def _stop_instance(self, instance_id: str) -> None:
        try:
            self.instances[instance_id].stop()

        except ClientError as e:
            raise EC2InstanceStopError(
                f"Cannot stop instance {instance_id}. Reason: {str(e)}")

    def stop_instance_by_id(self, instance_id: str) -> None:
        if not self._check_instance_exists(instance_id):
            raise EC2InstanceNotFoundError(
                f"Instance {instance_id} not found.")

        if (state := self._get_instance_state(instance_id)) != "running":
            raise EC2InstanceStateError(
                f"Cannot stop {state} instance.")

        self._stop_instance(instance_id)

    def verify_apache_is_running(self, instance_id: str) -> Tuple[bool, str]:
        public_ip = self.instances[instance_id].public_ip_address
        resp = requests.get(f"http://{public_ip}")
        return resp.status_code == 200, resp.content

    def download_file_from_instance(self, instance_id: str, path: str):
        ctx = paramiko.SSHClient()
        ctx.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        pkey_path = "./my-lab04-vm-access-key.pem"
        pkey = paramiko.RSAKey.from_private_key_file(pkey_path)

        ctx.connect(
            self.instances[instance_id].public_dns_name,
            username='ubuntu',
            pkey=pkey
        )
        stdin, stdout, _ = ctx.exec_command(f"cat {path}")
        stdin.close()

        return stdout.readlines()


class ClientAWS:
    session: boto3.session.Session
    ec2: ClientEC2

    def __init__(self):
        self.session = boto3.Session(region_name='us-east-1')
        self.ec2 = ClientEC2(self.session)


def lab04_scenario():
    INSTANCE_ID = 'i-02058e7ba63a9af6a'

    client = ClientAWS()
    client.ec2.start_instance_by_id(INSTANCE_ID)

    time.sleep(10)  # Wait for Apache to autostart
    apache_is_running, webpage_content = \
        client.ec2.verify_apache_is_running(INSTANCE_ID)
    assert apache_is_running is True
    pprint(webpage_content)

    file_content = client.ec2.download_file_from_instance(
        INSTANCE_ID, "/var/www/html/index.html")
    pprint(file_content)

    client.ec2.stop_instance_by_id(INSTANCE_ID)


if __name__ == "__main__":
    lab04_scenario()
