import json
import os
import re
import time
import uuid
from concurrent.futures.thread import ThreadPoolExecutor

import tornado.websocket
from sqlalchemy import create_engine, text
import aiofiles
import shutil
from loguru import logger

import engine_worker

# Configuration


_client_id_list = os.environ.get("CLIENT_ID_LIST")
_client_ids = set()
if _client_id_list is not None:
    _client_ids = set(_client_id_list.split(","))

_worker_id_list = os.environ.get("WORKER_ID_LIST")
_worker_ids = set()
if _worker_id_list is not None:
    _worker_ids = set(_worker_id_list.split(","))

# Init
_base_dir = os.path.dirname(os.path.abspath(__file__))
_engine_data_dir = os.path.join(_base_dir, "engine_data")

# avoid database too large, so we store job data in file system
_jobs_data_dir = os.path.join(_engine_data_dir, "jobs_data")

_files_dir = os.path.join(_engine_data_dir, "files")

if not os.path.exists(_engine_data_dir):
    os.mkdir(_engine_data_dir)
if not os.path.exists(_jobs_data_dir):
    os.mkdir(_jobs_data_dir)
if not os.path.exists(_files_dir):
    os.mkdir(_files_dir)

_engine = create_engine("sqlite:///" + _engine_data_dir + "/main.db", echo=True)
with _engine.connect() as create_con:
    create_con.exec_driver_sql(
        "create table if not exists job ("
        "  id INTEGER primary key autoincrement not null default 0"
        ", job_id TEXT not null default \"\""
        ", job_type INTEGER not null default 0"  # 0 unknown, 1 text to image, 2 image to text, 10000 custom task
        ", client_id TEXT not null default \"\""
        ", status INTEGER not null default 0"  # 0 unknown, 1 created, 2 processing, 3 finished, 4 failed
        ", create_ts_ms INTEGER not null default 0"
        ", update_ts_ms INTEGER not null default 0"
        ", title TEXT not null default \"\""
        ")"
    )
    create_con.commit()

_service_executor = ThreadPoolExecutor(6)
_current_io_loop = tornado.ioloop.IOLoop.current()
_current_io_loop.set_default_executor(executor=_service_executor)

# Utils
_job_id_valid = re.compile(r'[a-zA-Z0-9-]+$')


def valid_input_id(input_id):
    if not _job_id_valid.match(input_id):
        raise ValueError("job_id must be a-z A-Z 0-9 -")
    else:
        pass


# Database


class JobStorage:
    # single db thread executor for db operation to avoid db lock

    _db_executor = ThreadPoolExecutor(1)

    @classmethod
    def get_jobs(cls, client_id, job_type):
        f = cls._db_executor.submit(JobStorage._get_jobs, client_id, job_type)
        return f.result()

    @classmethod
    def _get_jobs(cls, client_id, job_type):
        with _engine.connect() as con:
            cur = con.execute(
                text(
                    "select"
                    " job_id"
                    ",job_type"
                    ",client_id"
                    ",status"
                    ",create_ts_ms"
                    ",update_ts_ms"
                    ",title"
                    " from job"
                    " where client_id = :client_id"
                    " and job_type = :job_type"
                    " order by create_ts_ms desc"
                    " limit 100"
                ),
                {
                    "client_id": client_id,
                    "job_type": job_type
                }
            )
            result = cur.fetchall()
            cur.close()
            con.commit()
            if result is None:
                return []
            else:
                return [
                    {
                        "job_id": x[0],
                        "job_type": x[1],
                        "client_id": x[2],
                        "status": x[3],
                        "create_ts_ms": x[4],
                        "update_ts_ms": x[5],
                        "title": x[6],
                    }
                    for x in result
                ]

    @classmethod
    def get_job(cls, job_id):
        f = cls._db_executor.submit(JobStorage._get_job, job_id)
        return f.result()

    @classmethod
    def _get_job(cls, job_id):
        with _engine.connect() as con:
            cur = con.execute(
                text(
                    "select"
                    " job_id"
                    ",job_type"
                    ",client_id"
                    ",status"
                    ",create_ts_ms"
                    ",update_ts_ms"
                    ",title"
                    " from job where"
                    " job_id = :job_id"
                ),
                {"job_id": job_id}
            )
            result = cur.fetchone()
            if result is None:
                return None

            job = {
                "job_id": result[0],
                "job_type": result[1],
                "client_id": result[2],
                "status": result[3],
                "create_ts_ms": result[4],
                "update_ts_ms": result[5],
                "title": result[6],
            }
            cur.close()
            con.commit()

            job_data_dir = os.path.join(_jobs_data_dir, job_id)
            job_param_file = os.path.join(job_data_dir, "job_param.json")
            job_result_file = os.path.join(job_data_dir, "job_result.json")

            if os.path.exists(job_param_file):
                with open(job_param_file, "r") as f:
                    param = json.loads(f.read())
                    job["param"] = param
            if os.path.exists(job_result_file):
                with open(job_result_file, "r") as f:
                    result = json.loads(f.read())
                    job["result"] = result

            return job

    @classmethod
    def create_job(cls, job):
        f = cls._db_executor.submit(JobStorage._create_job, job)
        return f.result()

    @classmethod
    def _create_job(cls, job):

        job_id = job["job_id"]
        job_data_dir = os.path.join(_jobs_data_dir, job_id)
        if os.path.exists(job_data_dir):
            shutil.rmtree(job_data_dir)
        os.mkdir(job_data_dir)
        job_param_file = os.path.join(job_data_dir, "job_param.json")
        with open(job_param_file, "w") as f:
            f.write(json.dumps(job["param"]))

        with _engine.connect() as con:
            con.execute(
                text(
                    "insert into job "
                    "( job_id, job_type, client_id, status, create_ts_ms, update_ts_ms, title ) "
                    "values "
                    "(:job_id, :job_type, :client_id, :status, :create_ts_ms, :update_ts_ms, :title )"
                ),
                {
                    "job_id": job_id,
                    "job_type": job["job_type"],
                    "client_id": job["client_id"],
                    "status": 1,
                    "create_ts_ms": int(time.time() * 1000),
                    "update_ts_ms": int(time.time() * 1000),
                    "title": job["title"],
                }
            )
            con.commit()

    @classmethod
    def get_undo_jobs_and_set_processing(cls, job_type, size):
        f = cls._db_executor.submit(JobStorage._get_undo_jobs_and_set_processing, job_type, size)
        return f.result()

    @classmethod
    def _get_undo_jobs_and_set_processing(cls, job_type, size):
        with _engine.connect() as con:
            cur = con.execute(
                text(
                    "select"
                    " job_id"
                    ",job_type"
                    ",client_id"
                    ",status"
                    ",create_ts_ms"
                    ",update_ts_ms"
                    ",title"
                    " from job where"
                    " status = 1 "
                    " and job_type = :job_type"
                    " order by create_ts_ms desc"
                    " limit :size"
                ),
                {"job_type": job_type, "size": size, "update_ts_ms": int(time.time() * 1000) - 10 * 60 * 1000}
            )
            result = cur.fetchall()
            cur.close()
            con.commit()
            if result is None:
                return []

            for x in result:
                con.execute(
                    text(
                        "update job "
                        " set status = :status, update_ts_ms = :update_ts_ms"
                        " where job_id = :job_id"
                    ),
                    {
                        "job_id": x[0],
                        "status": 2,
                        "update_ts_ms": int(time.time() * 1000),
                    }
                )
                con.commit()

            jobs = []
            for x in result:
                job = {
                    "job_id": x[0],
                    "job_type": x[1],
                    "client_id": x[2],
                    "status": x[3],
                    "create_ts_ms": x[4],
                    "update_ts_ms": x[5],
                    "title": x[6],
                }

                job_data_dir = os.path.join(_jobs_data_dir, job["job_id"])
                job_param_file = os.path.join(job_data_dir, "job_param.json")

                if os.path.exists(job_param_file):
                    with open(job_param_file, "r") as f:
                        param = json.loads(f.read())
                        job["param"] = param

                jobs.append(job)
            return jobs

    @classmethod
    def finish_job(cls, job):
        f = cls._db_executor.submit(JobStorage._finish_job, job)
        return f.result()

    @classmethod
    def _finish_job(cls, job):
        job_id = job["job_id"]
        job_data_dir = os.path.join(_jobs_data_dir, job_id)
        job_result_file = os.path.join(job_data_dir, "job_result.json")
        if os.path.exists(job_result_file):
            os.remove(job_result_file)
        with open(job_result_file, "w") as f:
            f.write(json.dumps(job["result"]))
        cls._update_job_status(job_id, job["status"])

    @classmethod
    def update_job_status(cls, job_id, status):
        f = cls._db_executor.submit(JobStorage._update_job_status, job_id, status)
        return f.result()

    @classmethod
    def _update_job_status(cls, job_id, status):
        with _engine.connect() as con:
            con.execute(
                text(
                    "update job "
                    " set status = :status, update_ts_ms = :update_ts_ms"
                    " where job_id = :job_id"
                ),
                {
                    "job_id": job_id,
                    "status": status,
                    "update_ts_ms": int(time.time() * 1000),
                }
            )
            con.commit()


# Service


class JobService:

    @classmethod
    def create_job(cls, job):
        valid_input_id(job["job_id"])
        JobStorage.create_job(job)

    @classmethod
    def get_undo_jobs_and_set_processing(cls, job_type):
        jobs = JobStorage.get_undo_jobs_and_set_processing(job_type, 1)
        if len(jobs) > 0:
            for job in jobs:
                cls.notify_client(job["client_id"], job["job_id"], job["job_type"], job["status"])

        return jobs

    @classmethod
    def update_job_status(cls, job_id, status):
        JobStorage.update_job_status(job_id, status)
        job = JobStorage.get_job(job_id)
        cls.notify_client(job["client_id"], job["job_id"], job["job_type"], job["status"])
        return job

    @classmethod
    def finish_job(cls, job):
        JobStorage.finish_job(job)
        job = JobStorage.get_job(job["job_id"])
        cls.notify_client(job["client_id"], job["job_id"], job["job_type"], job["status"])

    @classmethod
    def notify_client(cls, client_id, job_id, job_type, status):
        clients = _ws_client_id_to_ws_client_dict.get(client_id, set())
        content = {
            "job_id": job_id,
            "job_type": int(job_type),
            "status": int(status),
        }
        content_str = json.dumps(content)
        for client in clients:
            _current_io_loop.add_callback(cls._notify_client, client, content_str)

    @classmethod
    def _notify_client(cls, client, content_str):
        logger.info("notify client: %s" % content_str)
        try:
            client.write_message(json.dumps({
                "type": "content",
                "content": content_str,
            }))
        except Exception as e:
            logger.info(e)
            if client in _ws_ws_client_to_client_id_dict:
                client_id = _ws_ws_client_to_client_id_dict.get(client, None)
                del _ws_ws_client_to_client_id_dict[client]
                if client_id in _ws_client_id_to_ws_client_dict:
                    _ws_client_id_to_ws_client_dict[client_id].remove(client)

    @staticmethod
    def get_job(job_id):
        valid_input_id(job_id)
        job = JobStorage.get_job(job_id)
        return job

    @staticmethod
    def get_jobs(client_id, job_type):
        return JobStorage.get_jobs(client_id, job_type)


# WebSocket
_ws_ws_client_to_client_id_dict = dict()  # Handler
_ws_client_id_to_ws_client_dict = dict()


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        logger.info("WebSocket opened")

    def on_message(self, message):
        logger.info("WebSocket received message: %s" % message)
        msg = json.loads(message)
        client_id = msg["client_id"]
        clients = _ws_client_id_to_ws_client_dict.get(client_id, set())
        clients.add(self)
        _ws_client_id_to_ws_client_dict[client_id] = clients
        _ws_ws_client_to_client_id_dict[self] = client_id

    def on_close(self):
        logger.info("WebSocket closed")
        if self in _ws_ws_client_to_client_id_dict:
            client_id = _ws_ws_client_to_client_id_dict.get(self)
            del _ws_ws_client_to_client_id_dict[self]
            if client_id in _ws_client_id_to_ws_client_dict:
                _ws_client_id_to_ws_client_dict[client_id].remove(self)


# Web Handler

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        client_id = self.get_query_argument("client_id", "")
        if client_id == "":
            self.write("ok")
            return

        if len(_client_ids) > 0:
            if client_id not in _client_ids:
                self.set_status(403)
                self.write({"status": "error", "message": "client_id not allowed", "supported_job_types": [],
                            "custom_job_types": []})
                return
        self.write({
            "status": "ok",
            "supported_job_types": [1, 2, 10000],
            "custom_job_types": [
                {
                    "name": "Custom AI Engine task",
                    "type": 10000,
                },
                {
                    "name": "Test",
                    "type": 10001,
                },
                {
                    "name": "Test2",
                    "type": 10002,
                }
            ]
        })


class GetJobsHandler(tornado.web.RequestHandler):
    async def get(self):
        client_id = self.get_query_argument("client_id")
        if len(_client_ids) > 0:
            if client_id not in _client_ids:
                self.set_status(403)
                self.write({"status": "error", "message": "client_id not allowed"})
                return

        job_type = self.get_query_argument("job_type")
        jobs = await _current_io_loop.run_in_executor(_service_executor, JobService.get_jobs, client_id, job_type)
        self.write({"status": "ok", "jobs": jobs})


class CreateJobHandler(tornado.web.RequestHandler):
    async def post(self):
        client_id = self.get_query_argument("client_id")
        if len(_client_ids) > 0:
            if client_id not in _client_ids:
                self.set_status(403)
                self.write({"status": "error", "message": "client_id not allowed"})
                return

        job = json.loads(self.request.body)
        job["client_id"] = client_id

        await _current_io_loop.run_in_executor(_service_executor, JobService.create_job, job)
        self.write({"status": "ok"})


class GetUndoJobsHandler(tornado.web.RequestHandler):
    async def get(self):
        worker_id = self.get_query_argument("worker_id")
        if len(_worker_ids) > 0:
            if worker_id not in _worker_ids:
                self.set_status(403)
                self.write({"status": "error", "message": "worker_id not allowed"})
                return

        job_type = self.get_query_argument("job_type")
        jobs = await _current_io_loop.run_in_executor(
            _service_executor,
            JobService.get_undo_jobs_and_set_processing,
            job_type
        )
        if jobs is None:
            self.write({"status": "ok", "jobs": []})
        else:
            self.write({"status": "ok", "jobs": jobs})


class GetJobHandler(tornado.web.RequestHandler):
    async def get(self):
        client_id = self.get_query_argument("client_id")
        if len(_client_ids) > 0:
            if client_id not in _client_ids:
                self.set_status(403)
                self.write({"status": "error", "message": "client_id not allowed"})
                return

        job_id = self.get_query_argument("job_id")
        job = await _current_io_loop.run_in_executor(_service_executor, JobService.get_job, job_id)
        if job is None or job["client_id"] != client_id:
            self.set_status(404)
            self.write({"status": "error", "message": "job not found"})
        else:
            self.write(job)


class FinishJobHandler(tornado.web.RequestHandler):
    async def post(self):
        worker_id = self.get_query_argument("worker_id")
        if len(_worker_ids) > 0:
            if worker_id not in _worker_ids:
                self.set_status(403)
                self.write({"status": "error", "message": "worker_id not allowed"})
                return

        job = json.loads(self.request.body)
        await _current_io_loop.run_in_executor(_service_executor, JobService.finish_job, job)
        self.write({"status": "ok"})


class ReOpenJobHandler(tornado.web.RequestHandler):
    async def post(self):
        client_id = self.get_query_argument("client_id")
        if len(_client_ids) > 0:
            if client_id not in _client_ids:
                self.set_status(403)
                self.write({"status": "error", "message": "client_id not allowed"})
                return
        job_id = self.get_query_argument("job_id")
        job = await _current_io_loop.run_in_executor(_service_executor, JobService.update_job_status, job_id, 1)
        if job is None or job["client_id"] != client_id:
            self.set_status(404)
            self.write({"status": "error", "message": "job not found"})
        else:
            self.write({"status": "ok"})


@tornado.web.stream_request_body
class FileUploadHandler(tornado.web.RequestHandler):

    async def prepare(self):
        client_id = self.get_query_argument("client_id")
        if len(_client_ids) > 0:
            if client_id not in _client_ids:
                self.set_status(403)
                self.write({"status": "error", "message": "client_id not allowed"})
                return

        self.file_id = uuid.uuid4().hex
        file_path = os.path.join(_files_dir, self.file_id)
        self.file = await aiofiles.open(file_path, "wb")

    async def data_received(self, chunk):
        await self.file.write(chunk)

    async def post(self):
        await self.file.close()

        filename = self.get_query_argument("filename")
        content_type = self.request.headers["Content-Type"]
        encrypt = self.get_query_argument("encrypt", "0")
        meta = {
            "filename": filename,
            "content_type": content_type,
            "encrypt": encrypt
        }

        meta_file_path = os.path.join(_files_dir, self.file_id + "_meta.json")
        with open(meta_file_path, "w") as f:
            f.write(json.dumps(meta))
        self.write({
            "status": "ok",
            "file_id": self.file_id,
            "uri": "engine://" + self.file_id
        })


class FileDownloadHandler(tornado.web.RequestHandler):
    async def get(self):
        client_id = self.get_query_argument("client_id")

        if len(_client_ids) > 0:
            if client_id not in _client_ids:
                self.set_status(403)
                self.write({"status": "error", "message": "client_id not allowed"})
                return

        file_id = self.get_query_argument("file_id")
        if not _job_id_valid.match(file_id):
            raise ValueError("filename must be a-z A-Z 0-9 -")

        meta_file_path = os.path.join(_files_dir, file_id + "_meta.json")
        with open(meta_file_path, "r") as f:
            meta = json.loads(f.read())
            filename = meta.get("filename", file_id)
            self.set_header('Content-Type', meta["content_type"])
        # self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment; filename=' + filename)
        file_path = os.path.join(_files_dir, file_id)
        async with aiofiles.open(file_path, "rb") as f:
            while True:
                chunk = await f.read(1024)
                if not chunk:
                    break
                self.write(chunk)
        await self.finish()


application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/create_job", CreateJobHandler),
    (r"/get_undo_jobs", GetUndoJobsHandler),
    (r"/finish_job", FinishJobHandler),
    (r"/get_job", GetJobHandler),
    (r"/get_jobs", GetJobsHandler),
    (r"/re_open_job", ReOpenJobHandler),

    (r"/upload_file", FileUploadHandler),
    (r"/download_file", FileDownloadHandler),

    (r"/ws", WebSocketHandler),

])
application.listen(7860)
logger.info("server started at port 7860")

_file_password = os.environ.get("FILE_PASSWORD")
_huggingface_token = os.environ.get("HUGGINGFACE_TOKEN")
if _huggingface_token is not None:
    engine_worker.HuggingfaceWorker(JobService, _files_dir, _file_password, _huggingface_token).start()
    logger.info("huggingface worker started")

engine_worker.CustomWorker(JobService, _files_dir).start()

_current_io_loop.start()
