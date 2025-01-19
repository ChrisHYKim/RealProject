import subprocess


# backend-webserver 구성
def run_backend():
    try:
        back_process = subprocess.Popen(
            [
                "hypercorn",
                "/home/chris/Documents/PythonProject/Projects/backend-server/backend-fast:app",
                "--reload",
            ]
        )
        back_process.wait()
    except Exception as err:
        print(err)


if __name__ == "__main__":
    run_backend()
