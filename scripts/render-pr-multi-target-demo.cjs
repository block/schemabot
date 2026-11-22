#!/usr/bin/env node
// Render the multi-target PR timelines (every target, --target, and a stopped
// and resumed rollout) with one reusable browser session. Requires Playwright and ImageMagick.
const {renderAll}=require('./pr-demo-gif.cjs');
// Output seconds to scene seconds. Reading pauses stay; the polls play at one
// snapshot a second so the targets visibly move at their own pace.
const rolloutTiming=[[0,0],[2,3],[4,7],[5,9],[6.5,10],[16.5,20],[17.5,21.5],[18.5,23],[20.5,25],[22.5,29],[25.5,32]];
// The stop recording opens mid-rollout, slows down to read the stopped
// summary and the check it holds, and plays the resumed copy at one snapshot a second.
const stopTiming=[[0,0],[3.5,3.5],[6.5,7.5],[8,9.5],[10,10.5],[12,12],[15,15.5],[16.5,17.5],[25.5,26.5],[27,28],[29,30],[32,33]];
// Each check is [output second, element, texts the frame must show].
const opening=[[4,'#checks',['1 apply pending','Merging is blocked']]];
const scenarios=[
 {name:'multiTarget',page:'pr-multi-target-demo.html',gif:'assets/pr-multi-target-demo.gif',diff:'idx_status',timing:rolloutTiming,intro:4,length:25.5,
  checks:[...opening,[15,'#progress-body',['Rolling out','shop-004']],[29.5,'#checks',['All checks have passed','Pull request successfully merged']]]},
 // A narrowed apply never merges, so its recording ends on the blocked check.
 {name:'target',page:'pr-multi-target-demo.html',gif:'assets/pr-target-demo.gif',diff:'idx_status',timing:rolloutTiming,intro:4,length:21.5,
  checks:[...opening,[15,'#progress-body',['In Progress']],[25.5,'#checks',['1 apply pending','Merging is blocked']]]},
 {name:'stop',page:'pr-multi-target-stop-demo.html',gif:'assets/pr-multi-target-stop-demo.gif',timing:stopTiming,intro:0,length:32,
  checks:[[0,'#checks',['Apply in progress','Merging is blocked']],
   [10,'#stopped-body',['Schema Change Stopped','3 stopped','schemabot start apply-example-91 -e production']],
   [10,'#progress-body',['Stopped','shop-004']],[11,'#checks',['1 apply stopped','Merging is blocked']],
   [20,'#progress-body',['Schema change resumed','a new progress comment']],[20,'#resumed-body',['Rolling out','shop-001']],
   [32,'#checks',['All checks have passed','Pull request successfully merged']]]},
];
renderAll(scenarios,'pr-multi-target-demo').catch(e=>{console.error(e);process.exitCode=1});
