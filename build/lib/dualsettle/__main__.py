import argparse,csv,json,tempfile
from pathlib import Path
from .store import connect
from .ingest import stream,batch
from .rules import reconcile,close
from .report import write
def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='dualsettle-'));root.mkdir(parents=True,exist_ok=True)
    db=root/'data.db';con=connect(db)
    with con:
        con.execute("INSERT OR IGNORE INTO window VALUES ('D1','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
        con.execute("INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES ('M1','2025-01-01T00:00:00+00:00','2027-01-01T00:00:00+00:00','0.01')")
    con.close()
    events=root/'events.jsonl';events.write_text('\n'.join(json.dumps({'merchant':'M1','tx':'T1','version':n,'kind':kind,'amount':'100.00','business_at':f'2026-01-01T0{n}:00:00+00:00'}) for n,kind in [(1,'authorize'),(2,'confirm')]),encoding='utf-8')
    lines=root/'batch.csv'
    with lines.open('w',newline='',encoding='utf-8') as f:
        w=csv.writer(f);w.writerow(['id','merchant','amount','corrects']);w.writerow(['B1','M1','99.00',''])
    stream(events,db);batch(lines,db,'D1');run,_=reconcile(db,'D1');close(db,run)
    return write(db,run,root/'report')
def main():
    p=argparse.ArgumentParser(description='离线实时轨与日终轨封账核对');s=p.add_subparsers(dest='cmd',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    i=s.add_parser('ingest-stream');i.add_argument('file');i.add_argument('--db',required=True)
    b=s.add_parser('ingest-batch');b.add_argument('file');b.add_argument('--db',required=True);b.add_argument('--window',required=True)
    r=s.add_parser('reconcile');r.add_argument('--db',required=True);r.add_argument('--window',required=True)
    c=s.add_parser('close');c.add_argument('--db',required=True);c.add_argument('--run',type=int,required=True)
    q=s.add_parser('report');q.add_argument('--db',required=True);q.add_argument('--run',type=int,required=True);q.add_argument('--out',required=True)
    a=p.parse_args()
    if a.cmd=='demo':print(demo(a.workdir))
    elif a.cmd=='ingest-stream':print(stream(a.file,a.db))
    elif a.cmd=='ingest-batch':print(batch(a.file,a.db,a.window))
    elif a.cmd=='reconcile':print(reconcile(a.db,a.window))
    elif a.cmd=='close':close(a.db,a.run)
    else:print(write(a.db,a.run,a.out))
if __name__=='__main__':main()
