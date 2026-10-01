import io
import zipfile

import pytest

from credito_pa.common.download import InvalidFileError, download_file, is_valid_zip
from credito_pa.common.http import HttpClient


def zip_bytes(name="a.csv", content="x;y\n1;2\n"):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(name, content)
    return buf.getvalue()


def test_html_no_lugar_de_zip_vira_invalid_zip(tmp_path, fake_session, fake_response):
    sess = fake_session([fake_response(200, text="<html>pagina de erro</html>")])
    client = HttpClient(session=sess, sleep=lambda s: None)
    with pytest.raises(InvalidFileError) as err:
        download_file(client, "http://x/202412_ESTBAN.ZIP", tmp_path / "202412_ESTBAN.ZIP")
    assert err.value.reason == "invalid_zip"
    assert err.value.path.exists()
    assert not (tmp_path / "202412_ESTBAN.ZIP").exists()


def test_cache_evita_novo_download(tmp_path, fake_session, fake_response):
    sess = fake_session([fake_response(200, content=zip_bytes())])
    client = HttpClient(session=sess, sleep=lambda s: None)
    dest = tmp_path / "f.zip"
    _, baixou1 = download_file(client, "http://x/f.zip", dest)
    _, baixou2 = download_file(client, "http://x/f.zip", dest)
    assert (baixou1, baixou2) == (True, False)
    assert len(sess.calls) == 1
    assert is_valid_zip(dest)
