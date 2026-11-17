import hashlib
def event_identity(e):
    return str(e['merchant']),str(e['tx']),int(e['version'])
def content_hash(line):return hashlib.sha256(line.encode('utf-8')).hexdigest()
