#!/usr/bin/env node
// Render the multi-target PR timelines (every target, --target, and a stopped
// and resumed rollout) with one reusable browser session. Requires Playwright and ImageMagick.
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {execFileSync}=require('node:child_process');
const {pathToFileURL}=require('node:url');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'..');
// Output seconds to scene seconds. Reading pauses stay; the polls play at one
// snapshot a second so the targets visibly move at their own pace.
const rolloutTiming=[[0,0],[2,3],[4,7],[5,9],[6.5,10],[16.5,20],[17.5,21.5],[18.5,23],[20.5,25],[22.5,29],[25.5,32]];
// The stop recording opens mid-rollout, slows down to read the stopped
// summary and the check it holds, and plays the resumed copy at one snapshot a second.
const stopTiming=[[0,0],[3.5,3.5],[6.5,7.5],[8,9.5],[10,10.5],[12,12],[15,15.5],[16.5,17.5],[25.5,26.5],[27,28],[29,30],[32,33]];
// Each check is [output second, element, texts the frame must show].
const opening=[[4,'#checks',['1 apply pending','Merging is blocked']]];
const scenarios=[
 {name:'multiTarget',page:'pr-multi-target-demo.html',gif:'assets/pr-multi-target-demo.gif',timing:rolloutTiming,intro:4,length:25.5,
  checks:[...opening,[15,'#progress-body',['Rolling out','shop-004']],[29.5,'#checks',['All checks have passed','Pull request successfully merged']]]},
 // A narrowed apply never merges, so its recording ends on the blocked check.
 {name:'target',page:'pr-multi-target-demo.html',gif:'assets/pr-target-demo.gif',timing:rolloutTiming,intro:4,length:21.5,
  checks:[...opening,[15,'#progress-body',['In Progress']],[25.5,'#checks',['1 apply pending','Merging is blocked']]]},
 {name:'stop',page:'pr-multi-target-stop-demo.html',gif:'assets/pr-multi-target-stop-demo.gif',timing:stopTiming,intro:0,length:32,
  checks:[[0,'#checks',['Apply in progress','Merging is blocked']],
   [10,'#stopped-body',['Schema Change Stopped','3 stopped','schemabot start apply-example-91 -e production']],
   [10,'#progress-body',['Stopped','shop-004']],[11,'#checks',['1 apply stopped','Merging is blocked']],
   [20,'#progress-body',['Schema change resumed','a new progress comment']],[20,'#resumed-body',['Rolling out','shop-001']],
   [32,'#checks',['All checks have passed','Pull request successfully merged']]]},
];
const fps=20,holdSeconds=2.5;
function sceneTime(scenario,seconds){
 if(seconds<scenario.intro)return -1;
 seconds-=scenario.intro;
 const timing=scenario.timing;
 for(let j=1;j<timing.length;j++){
  const [end,to]=timing[j],[start,from]=timing[j-1];
  if(seconds<=end)return from+(to-from)*(seconds-start)/(end-start);
 }
 return timing[timing.length-1][1];
}
async function render(page,scenario,tmp){
 const dir=fs.mkdtempSync(path.join(tmp,scenario.name+'-'));
 // A hash-only change would not reload the page, so start each scenario fresh.
 await page.goto('about:blank');
 await page.goto(pathToFileURL(path.join(root,'assets/src',scenario.page)).href+'#'+scenario.name);
 await page.evaluate(()=>Promise.all([...document.images].map(i=>i.decode())));
 const duration=scenario.intro+scenario.length;
 const frames=[];let lastCamera;
 for(let i=0;i<=duration*fps;i++){
  // Hold the settled final view without repainting identical DOM each frame.
  if(i>(duration-holdSeconds)*fps){frames.push(frames[frames.length-1]);continue;}
  const t=sceneTime(scenario,i/fps);
  await page.evaluate(({t,dt})=>window.renderFrame(t,dt),{t,dt:1/fps});
  // A comment that folds above the camera shifts the thread; the page
  // compensates, so add that back before checking the motion.
  const camera=await page.evaluate(()=>previousScroll+(typeof scrollAdjust==='number'?scrollAdjust:0));
  if(t>=7&&lastCamera!==undefined&&(camera<lastCamera-1||camera-lastCamera>26))throw new Error(scenario.name+': scrolling must move forward without jumps');
  lastCamera=camera;
  if(i===0&&scenario.intro&&!(await page.locator('.code.added').allTextContents()).join(' ').includes('idx_status'))throw new Error(scenario.name+': opening frame must show the index diff');
  for(const [second,selector,wants] of scenario.checks){
   if(i!==Math.round(Math.min(second,duration-holdSeconds)*fps))continue;
   const text=await page.locator(selector).innerText();
   for(const want of wants)if(!text.includes(want))throw new Error(`${scenario.name}: ${selector} at ${second}s must show ${want}`);
  }
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  const frame=path.join(dir,`${String(i).padStart(4,'0')}.png`);await page.screenshot({path:frame});frames.push(frame);
 }
 const out=path.join(root,scenario.gif);
 // A shared palette keeps text and neutral backgrounds from shimmering, and
 // fixed swatches keep GitHub's diff and UI colors exact.
 const fixedColors=['ffffff','f6f8fa','ffebe9','ffcecb','dafbe1','aceebb','ddf4ff','d1d9e0','1f2328','59636e','cf222e','1a7f37','0969da','8250df','fbefff','9a6700'];
 const swatches=path.join(dir,'fixed.ppm'),adaptive=path.join(dir,'adaptive.png'),palette=path.join(dir,'palette.png');
 fs.writeFileSync(swatches,`P3\n${fixedColors.length} 1\n255\n`+fixedColors.map(hex=>hex.match(/../g).map(v=>parseInt(v,16)).join(' ')).join('\n'));
 execFileSync('magick',[...frames.filter((_,i)=>i%fps===0),'-append','-colors',String(256-fixedColors.length),'-unique-colors',adaptive]);
 execFileSync('magick',[adaptive,swatches,'+append','-unique-colors',palette]);
 execFileSync('magick',['-delay','5','-loop','0',...frames,'+dither','-remap',palette,'-layers','OptimizePlus',out]);
 const timeline=execFileSync('magick',[out,'-coalesce','-format','%T %#\n','info:'],{encoding:'utf8'}).trim().split('\n').map(line=>line.split(' '));
 let held=0;const finalHash=timeline[timeline.length-1][1];
 for(const [delay,hash] of timeline.reverse()){
  if(held>=200)break;
  if(hash!==finalHash)throw new Error(scenario.name+': final GIF reading pause must stay still');
  held+=Number(delay);
 }
 if(held<200)throw new Error(scenario.name+': final GIF reading pause must last two seconds');
 console.log(`Rendered ${scenario.gif}: ${frames.length} frames, about ${duration} seconds`);
}
(async()=>{
 const tmp=fs.mkdtempSync(path.join(os.tmpdir(),'pr-multi-target-demo-'));
 const chrome=process.env.CHROME||'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
 let browser;
 try {
  browser=await chromium.launch({headless:true,...(fs.existsSync(chrome)?{executablePath:chrome}:{})});
  const page=await browser.newPage({viewport:{width:1100,height:820},deviceScaleFactor:1});
  const only=process.argv[2];
  for(const scenario of scenarios)if(!only||only===scenario.name)await render(page,scenario,tmp);
 } finally {if(browser)await browser.close();fs.rmSync(tmp,{recursive:true,force:true})}
})().catch(e=>{console.error(e);process.exitCode=1});
