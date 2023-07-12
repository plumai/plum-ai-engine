import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests
from loguru import logger


class HuggingfaceWorker:

    def __init__(self, job_service_cls, _files_dir, _huggingface_token):
        self.job_service_cls = job_service_cls
        self._files_dir = _files_dir
        self._huggingface_token = _huggingface_token
        self.__worker_executor = ThreadPoolExecutor(1)

    def start(self):
        self.__worker_executor.submit(self.run, self.job_service_cls)

    def run(self, job_service_cls):

        job_types = [1, 2]
        i = 0
        while True:
            i = i + 1
            if i >= len(job_types):
                i = i % len(job_types)

            job_type = job_types[i]
            try:
                jobs = job_service_cls.get_undo_jobs_and_set_processing(job_type)
                if len(jobs) == 0:
                    logger.info("No job found, sleep 5 seconds, job_type: " + str(job_type))
                    time.sleep(5)
                    continue
                for job in jobs:
                    try:
                        if job_type == 1:
                            (image_url, filename) = self._text_to_image(job)
                            job_result = dict()
                            job_result["contents"] = []
                            job_result["attachment_uris"] = []
                            job_result["attachment_uris"].append(image_url)
                            job["result"] = job_result
                            job["status"] = 3
                            job_service_cls.finish_job(job)
                        elif job_type == 2:
                            content = self._image_to_text(job)
                            job_result = dict()
                            job_result["contents"] = []
                            job_result["contents"].append(content)
                            job_result["attachment_uris"] = []
                            job["result"] = job_result
                            job["status"] = 3
                            job_service_cls.finish_job(job)
                    except Exception as e:
                        logger.error("Error: " + str(e))
                        job_result = dict()
                        job_result["contents"] = []
                        job_result["attachment_uris"] = []
                        job_result["error"] = str(e)
                        job["result"] = job_result
                        job["status"] = 4
                        job_service_cls.finish_job(job)
                        time.sleep(30)
            except Exception as e:
                logger.error("Error: " + str(e))
                time.sleep(30)

    def _text_to_image(self, job):
        api_url = "https://api-inference.huggingface.co/models/runwayml/stable-diffusion-v1-5"
        headers = {"Authorization": "Bearer " + self._huggingface_token}

        payload = {
            "inputs": job["param"]["contents"][0],
        }
        response = requests.post(api_url, headers=headers, json=payload, timeout=60)

        if response.status_code == 503:
            raise Exception("Huggingface API is not available")

        if response.status_code >= 400:
            raise Exception(response.text)

        image_bytes = response.content

        file_id = uuid.uuid4().hex
        filename = file_id + ".png"

        file_path = os.path.join(self._files_dir, file_id)
        with open(file_path, "wb") as f:
            f.write(image_bytes)

        meta = {
            "filename": filename,
            "content_type": "image/x-png"
        }

        meta_file_path = os.path.join(self._files_dir, file_id + "_meta.json")
        with open(meta_file_path, "w") as f:
            f.write(json.dumps(meta))

        return "engine://" + file_id, filename

    def _image_to_text(self, job):
        api_url = "https://api-inference.huggingface.co/models/Salesforce/blip-image-captioning-base"
        headers = {"Authorization": "Bearer " + self._huggingface_token}

        uri = job["param"]["attachment_uris"][0]
        file_id = uri.replace("engine://", "")
        file_path = os.path.join(self._files_dir, file_id)
        with open(file_path, "rb") as f:
            data = f.read()

        response = requests.post(api_url, headers=headers, data=data, timeout=60)
        if response.status_code == 503:
            raise Exception("Huggingface API is not available")

        if response.status_code >= 400:
            raise Exception(response.text)
        return response.json()[0]["generated_text"]


class CustomWorker:

    def __init__(self, job_service_cls, _files_dir):
        self.job_service_cls = job_service_cls
        self._files_dir = _files_dir
        self.__worker_executor = ThreadPoolExecutor(1)

    def start(self):
        self.__worker_executor.submit(self.run, self.job_service_cls)

    def run(self, job_service_cls):

        job_types = [10000]
        i = 0
        while True:
            i = i + 1
            if i >= len(job_types):
                i = i % len(job_types)

            job_type = job_types[i]
            try:
                jobs = job_service_cls.get_undo_jobs_and_set_processing(job_type)
                if len(jobs) == 0:
                    logger.info("No job found, sleep 5 seconds, job_type: " + str(job_type))
                    time.sleep(5)
                    continue

                for job in jobs:
                    job_result = dict()
                    job_result["contents"] = []
                    job_result["contents"].append("Not implemented, you can implement it in custom worker")
                    job_result["attachment_uris"] = []
                    job["result"] = job_result
                    job["status"] = 3
                    job_service_cls.finish_job(job)
            except Exception as e:
                logger.error("Error: " + str(e))
                time.sleep(30)
