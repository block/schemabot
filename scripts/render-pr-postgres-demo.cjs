#!/usr/bin/env node
// Render the PostgreSQL PR timeline: a column add and a concurrent index
// build, each its own task, through to merge. Requires Playwright and ImageMagick.
const {renderAll}=require('./pr-demo-gif.cjs');
// Output seconds to scene seconds. Reading pauses stay; the polls play at one
// snapshot a second so each index build phase can be read.
const timing=[[0,0],[2,3],[4,7],[5,9],[6.5,10],[17.5,21],[18.5,21.5],[19.5,23],[21.5,25],[23.5,29],[26.5,32]];
// Each check is [output second, element, texts the frame must show].
const scenarios=[
 {name:'postgres',page:'pr-postgres-demo.html',gif:'assets/pr-postgres-demo.gif',timing,intro:4,length:26.5,diff:'idx_users_last_seen_at',
  checks:[[4,'#checks',['1 apply pending','Merging is blocked']],
   [10.6,'#progress-body',['ALTER TABLE public.users ADD COLUMN last_seen_at']],
   [13,'#progress-body',['CREATE INDEX CONCURRENTLY','building index: 12% of blocks','1/2 complete']],
   [30.5,'#checks',['All checks have passed','Pull request successfully merged']]]},
];
renderAll(scenarios,'pr-postgres-demo').catch(e=>{console.error(e);process.exitCode=1});
