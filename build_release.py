"""Build only reviewed public files; deployment configuration is never bundled."""
from pathlib import Path, PurePosixPath
import datetime
import hashlib
import io
import json
import re
import tarfile


def source_files(root):
    root = Path(root).resolve()
    names = json.loads((root/'release-manifest.json').read_text(encoding='utf-8'))
    if len(names)!=len(set(names)):
        raise ValueError('duplicate manifest entries')
    files=[]
    for name in names:
        relative=PurePosixPath(name)
        if relative.is_absolute() or '..' in relative.parts or chr(92) in name:
            raise ValueError('unsafe public file path')
        path=root/name
        if not path.is_file() or path.is_symlink() or path.resolve()!=path:
            raise ValueError('missing or linked public file: '+name)
        lower = path.name.lower()
        private_env = (lower == '.env' or lower.endswith('.env') or '.env.' in lower or lower.startswith('.env.'))
        example_env = relative.parts[0] == 'examples' and lower.endswith('.env.example')
        if (path.suffix.lower() in {'.key','.pem','.crt','.csr','.p12','.pfx','.db','.sqlite3'}
                or (private_env and not example_env) or '.sqlite3-' in lower or '.db-' in lower
                or lower in {'cluster.json','chains.json'}
                or relative.parts[0].lower() in {'personal','run','private-backups','.git'}):
            raise ValueError('private file in public manifest: '+name)
        content=path.read_text(encoding='utf-8')
        if re.search(r'\b\d{6,12}:[A-Za-z0-9_-]{30,}\b',content) or re.search(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',content):
            raise ValueError('possible secret in public file: '+name)
        files.append(path)
    return files


def build(root=None, output=None):
    root=Path(root or Path(__file__).parent).resolve()
    files=source_files(root)
    archive=Path(output or root/'dist/crypto-monitor-v2.tar.gz')
    archive.parent.mkdir(parents=True,exist_ok=True)
    stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    with tarfile.open(archive,'w:gz') as tar:
        for path in files:
            content=path.read_bytes().replace(b'\r\n',b'\n')
            entry=tarfile.TarInfo(path.relative_to(root).as_posix())
            entry.size,entry.mode=len(content),0o644
            tar.addfile(entry,io.BytesIO(content))
        version=('crypto-monitor '+stamp+'\n').encode()
        entry=tarfile.TarInfo('RELEASE.txt');entry.size=len(version)
        tar.addfile(entry,io.BytesIO(version))
    digest=hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_name(archive.name+'.sha256').write_text(digest+'  '+archive.name+'\n',encoding='ascii')
    return 'release-'+stamp+'-'+digest[:8],digest


if __name__=='__main__':
    print('\n'.join(build()))
