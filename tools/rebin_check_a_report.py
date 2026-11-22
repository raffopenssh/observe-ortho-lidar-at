"""Check A sample report (docs/tree-cell-assignment.md -> Check A): 4 named + 25 frame + 25 plain KGs (seed 1).
Run from the repo root: nice python3 tools/rebin_check_a_report.py"""
import sys; sys.path.insert(0,'.'); sys.path.insert(0,'tools')
import numpy as np, ne_rebin, ne_pos_fix, random, pickle, glob, os
import rebin_check_a as A
fr = sorted(ne_pos_fix._load()['off']); random.seed(1)
allk = sorted(os.path.basename(p)[:-4] for p in glob.glob('data/ne/*.h3b') if len(os.path.basename(p))==9)
kgs = ['92124','01005','61047','30003'] + random.sample(fr, 25) + random.sample([k for k in allk if k not in fr], 25)
SH=[(5,-5),(-5,5),(5,5),(-5,-5),(7,0),(0,7)]
R=[]
for kg in kgs:
    st, off = ne_rebin.scan(kg)
    se, dist, dh, ddx, ddy, ok, ex = A.others(kg, off, SH)
    R.append(dict(kg=kg, st=st, frame=kg in fr, se=se, dist=dist, dh=dh, ddx=ddx, ddy=ddy, ok=ok, ex=ex, h=off['h_m'],
                  oframe=np.array([bool(ne_pos_fix.offset(str(int(k)).zfill(5))) for k in ok])))
pickle.dump(R, open('/tmp/checka.pkl','wb'))
import pickle, numpy as np
R = pickle.load(open('/tmp/checka.pkl','rb'))
DB=[0,1,2,3,5,1e9]; HB=[0.5,1,2,1e9]
def cat(k, rows): return np.concatenate([r[k] for r in rows])
def show(name, rows, sel=None):
    se=cat('se',rows); dist=cat('dist',rows); dh=np.abs(cat('dh',rows)); 
    m = se if sel is None else se & sel(rows)
    stored=sum(r['st']['stored'] for r in rows); off=se.size
    print(f'\n== {name}: {len(rows)} KGs, stored {stored}, off_file {off} ({100*off/stored:.3f} %), other_file {int(se.sum())}, no_row {int((~se).sum())}; shown {int(m.sum())}')
    hist=np.zeros((6,5),int)
    fin=np.isfinite(dist)
    di=np.where(dist==0,0,1+np.clip(np.searchsorted(DB,dist,side='left')-1,0,4)); hi=np.where(dh==0,0,1+np.searchsorted(HB,dh,side='left').clip(0,3))
    mm=m&fin
    np.add.at(hist,(di[mm],hi[mm]),1)
    print('   dist \\ |dh|     =0   <=0.5    <=1    <=2     >2')
    for i,l in enumerate(['=0','0-1','1-2','2-3','3-5','>5']): print(f'   {l:>5} m  '+''.join(f'{v:7d}' for v in hist[i]))
    print('   no apex of another file within reach:', int((m&~fin).sum()))
    # fine: Chebyshev
    dx=np.abs(cat('ddx',rows)); dy=np.abs(cat('ddy',rows)); ch=np.maximum(dx,dy)
    print('   Chebyshev max(|dx|,|dy|) m : n / share |dh|<=0.5 / share |dh|<=1')
    for c in range(0,9):
        k=mm&(ch==c)
        if k.any(): print(f'     {c}: {int(k.sum()):5d}  {100*(dh[k]<=0.5).mean():5.1f} %  {100*(dh[k]<=1).mean():5.1f} %')
    return m, ch, dh
plain=[r for r in R if not r['frame']]; frame=[r for r in R if r['frame']]
show('plain KGs', plain)
show('frame KGs, other file also frame', frame, lambda rows: cat('oframe',rows))
m,ch,dh=show('frame KGs, other file NOT frame', frame, lambda rows: ~cat('oframe',rows))
m,ch,dh=show('ALL', R)
# frame-offset test
for nm, rows, sel in (('frame src / plain other', frame, lambda rows: ~cat('oframe',rows)), ('frame/frame', frame, lambda rows: cat('oframe',rows)), ('plain src', plain, None)):
    se=cat('se',rows); m=se if sel is None else se&sel(rows)
    dx=np.abs(cat('ddx',rows)); dy=np.abs(cat('ddy',rows)); dh=np.abs(cat('dh',rows))
    notdup = m & ~((np.maximum(dx,dy)<=2)&(dh<=1))
    print(f'\n{nm}: not dup at 0 offset: {int(notdup.sum())}; of these with a |dh|<=0.25 apex within +-2 m of pos+shift:')
    for s in rows[0]['ex']:
        b=np.concatenate([r['ex'][s] for r in rows]); print('   shift',s, int((notdup&(b<=0.25)).sum()), ' exact dh=0:', int((notdup&(b==0)).sum()))
# thresholds
se=cat('se',R); dx=np.abs(cat('ddx',R)); dy=np.abs(cat('ddy',R)); dh=np.abs(cat('dh',R)); d=cat('dist',R)
for nm,k in (('2 m eucl, 1 m', (d<=2)&(dh<=1)), ('cheb 2 m (<=2.83), 1 m', (np.maximum(dx,dy)<=2)&(dh<=1)), ('cheb 2, 0.5', (np.maximum(dx,dy)<=2)&(dh<=0.5)), ('3 m, 1 m',(d<=3)&(dh<=1))):
    for g,rows in (('plain',plain),('frame',frame)):
        gm=np.concatenate([np.full(r['se'].size, r['frame']) for r in R])==(g=='frame')
        print(f'{nm:24s} {g}: dup {int((se&k&gm).sum())} / off {int(gm.sum())} = {100*(se&k&gm).sum()/gm.sum():.1f} %  (of other_file {100*(se&k&gm).sum()/(se&gm).sum():.1f} %)')
