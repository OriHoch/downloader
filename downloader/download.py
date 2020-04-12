import signal
from signal import SIGPIPE, SIG_IGN
import pycurl
import time


DEFAULT_MAX_REDIRECTS = 5
DEFAULT_CONNECT_TIMEOUT_SECONDS = 30
DEFAULT_TIMEOUT_SECONDS = 15
SLEEP_TIME_SECONDS_IF_NONE_RUNNING = 1


def download(concurrent_connections,
             iterator,
             save_result,
             max_redirects=DEFAULT_MAX_REDIRECTS,
             connect_timeout_seconds=DEFAULT_CONNECT_TIMEOUT_SECONDS,
             timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
             ):
    # We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
    # the libcurl tutorial for more info.
    signal.signal(SIGPIPE, SIG_IGN)
    curl_multi = pycurl.CurlMulti()
    curl_multi.handles = []
    for i in range(concurrent_connections):
        curl = pycurl.Curl()
        curl.fp = None
        curl.setopt(pycurl.FOLLOWLOCATION, 1)
        curl.setopt(pycurl.MAXREDIRS, max_redirects)
        curl.setopt(pycurl.CONNECTTIMEOUT, connect_timeout_seconds)
        curl.setopt(pycurl.TIMEOUT, int(timeout_seconds))
        curl.setopt(pycurl.NOSIGNAL, 1)
        curl_multi.handles.append(curl)
    try:
        freelist = curl_multi.handles[:]
        while True:
            while len(freelist) > 0:
                urlobj = next(iterator)
                if urlobj is None:
                    break
                else:
                    curl = freelist.pop()
                    curl.setopt(pycurl.URL, urlobj['url'])
                    curl.fp = open(urlobj['output_filename'], "wb")
                    curl.hfp = open(urlobj['header_filename'], "wb") if urlobj.get('header_filename') is not None else None
                    curl.setopt(pycurl.WRITEDATA, curl.fp)
                    if curl.hfp is not None:
                        curl.setopt(pycurl.WRITEHEADER, curl.hfp)
                    curl_multi.add_handle(curl)
                    curl.urlobj = urlobj
            if len(freelist) == concurrent_connections:
                time.sleep(SLEEP_TIME_SECONDS_IF_NONE_RUNNING)
            else:
                while True:
                    ret, num_running_handles = curl_multi.perform()
                    if ret != pycurl.E_CALL_MULTI_PERFORM:
                        break
                while True:
                    num_handles_in_queue, ok_list, err_list = curl_multi.info_read()
                    for curl in ok_list:
                        curl.fp.close()
                        curl.fp = None
                        if curl.hfp is not None:
                            curl.hfp.close()
                            curl.hfp = None
                        curl_multi.remove_handle(curl)
                        save_result(curl.urlobj, response_code=curl.getinfo(pycurl.RESPONSE_CODE))
                        curl.urlobj = None
                        freelist.append(curl)
                    for curl, errno, errmsg in err_list:
                        curl.fp.close()
                        curl.fp = None
                        if curl.hfp is not None:
                            curl.hfp.close()
                            curl.hfp = None
                        curl_multi.remove_handle(curl)
                        save_result(curl.urlobj, errno=errno, errmsg=errmsg)
                        curl.urlobj = None
                        freelist.append(curl)
                    if num_handles_in_queue == 0:
                        break
                curl_multi.select(1.0)
    finally:
        for curl in curl_multi.handles:
            if getattr(curl, 'fp', None) is not None:
                curl.fp.close()
                curl.fp = None
            if getattr(curl, 'hfp', None) is not None:
                curl.hfp.close()
                curl.hfp = None
            curl.urlobj = None
            curl.close()
        curl_multi.close()
