import os

configurations = {
    "engine_data_dir": os.path.join(os.path.dirname(os.path.abspath(__file__)), "engine_data"),
    "client_id_list": os.environ.get("CLIENT_ID_LIST"),
    "worker_id_list": os.environ.get("WORKER_ID_LIST"),
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
    ],
    "file_password": os.environ.get("FILE_PASSWORD"),
    "huggingface_token": os.environ.get("HUGGINGFACE_TOKEN"),
}
