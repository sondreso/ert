import luigi
import os
import time

def luigi_test():
    del os.environ["http_proxy"]
    del os.environ["HTTP_PROXY"]
    s = luigi.RemoteScheduler()
    while True:
        t = s.task_list()
        if t:
            print(t)
        time.sleep(1)
    print("HeiHei")

if __name__ == "__main__":
    luigi_test()
