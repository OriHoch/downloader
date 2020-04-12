import os
from downloader.download import download


def test_download(output_directory, concurrent_connections, multiply_urls, urls):

    def _iterator():
        os.makedirs(output_directory, exist_ok=True)
        for i, url in enumerate(urls):
            for j in range(int(multiply_urls)):
                ij = str(i)+'.'+str(j)
                yield {
                    'ij': ij,
                    'url': url,
                    'output_filename': os.path.join(output_directory, ij + '.output'),
                    'header_filename': os.path.join(output_directory, ij + '.header'),
                }
        for _ in range(5):
            yield None

    def _save_result(urlobj, response_code=None, errno=None, errmsg=None):
        print('%s %s: response_code=%s errno=%s errmsg=%s' % (urlobj['ij'], urlobj['url'], response_code, errno, errmsg))

    download(int(concurrent_connections), _iterator(), _save_result)


if __name__ == '__main__':
    test_download(".output", 2, 2, ["http://www.example.com", "http://www.example.com/foo", "https://www.example.com"])
