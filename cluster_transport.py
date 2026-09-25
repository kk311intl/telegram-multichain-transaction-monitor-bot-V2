"""Direct cluster transport: mutual TLS on public networks, explicit private HTTP opt-in."""
import ipaddress
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from functools import lru_cache


@lru_cache(maxsize=8)
def tls_context(ca, certificate, key, server=False):
    if not all((ca, certificate, key)):
        raise ValueError('CLUSTER_TLS_CA, CLUSTER_TLS_CERT and CLUSTER_TLS_KEY are required')
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH if server else ssl.Purpose.SERVER_AUTH,
                                         cafile=ca)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(certificate, key)
    return context


def configured_tls(server=False):
    return tls_context(*(os.environ.get('CLUSTER_TLS_'+name,'') for name in ('CA','CERT','KEY')),server)


def peer_node(connection):
    certificate = connection.getpeercert()
    names = [value for group in certificate.get('subject',()) for name,value in group if name=='commonName']
    return names[0] if len(names)==1 else None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(request.full_url,code,'cluster redirects forbidden',headers,fp)


def open_cluster(request, timeout=8):
    url = urllib.parse.urlsplit(request.full_url)
    if url.username or url.password or url.query or url.fragment:
        raise ValueError('invalid cluster URL')
    handlers = [urllib.request.ProxyHandler({}),NoRedirect()]
    if url.scheme=='https':
        handlers.append(urllib.request.HTTPSHandler(context=configured_tls()))
    elif url.scheme=='http' and os.environ.get('CLUSTER_ALLOW_PLAINTEXT')=='1':
        address = ipaddress.ip_address(url.hostname)
        if not (address.is_private or address in ipaddress.ip_network('100.64.0.0/10')):
            raise ValueError('plaintext cluster transport requires a private IP')
        if any(os.environ.get('CLUSTER_TLS_'+name) for name in ('CA','CERT','KEY')):
            raise ValueError('TLS configuration cannot fall back to HTTP')
    else:
        raise ValueError('use HTTPS with mutual TLS for cluster transport')
    return urllib.request.build_opener(*handlers).open(request,timeout=timeout)
