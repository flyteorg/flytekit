import os
import signal

if __name__ == "__main__":
    print("Terminating server and resuming task.")
    answer = (
        input(
            "This operation will kill the server. All unsaved data will be lost, and you will no longer be able to connect to it. Do you really want to terminate? (Y/N): "
        )
        .strip()
        .upper()
    )
    if answer == "Y":
        PID = 3428027
        os.kill(PID, signal.SIGTERM)
        print("The server has been terminated and the task has been resumed.")
    else:
        print("Operation canceled.")
