#!/usr/bin/env node
// Render the multi-target PR timelines (every target, then --target) with one
// reusable browser session. Requires Playwright and ImageMagick.
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {execFileSync}=require('node:child_process');
const {pathToFileURL}=require('node:url');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'..');
const scenarios=[
 {name:'multiTarget',gif:'assets/pr-multi-target-demo.gif',final:['All checks have passed','Pull request successfully merged'],progress:['Rolling out','shop-004']},
 {name:'target',gif:'assets/pr-target-demo.gif',final:['1 apply pending','Merging is blocked'],progress:['In Progress']},
];
// Output seconds to scene seconds. Reading pauses stay; the polls play at one
// snapshot a second so the targets visibly move at their own pace.
const timing=[[0,0],[2,3],[4,7],[5,9],[6.5,10],[16.5,20],[17.5,21.5],[18.5,23],[20.5,25],[22.5,29],[25.5,32]];
const fps=20,introSeconds=4,holdSeconds=2.5;
function sceneTime(seconds){
 if(seconds<introSeconds)return -1;
 seconds-=introSeconds;
 for(let j=1;j<timing.length;j++){
  const [end,to]=timing[j],[start,from]=timing[j-1];
  if(seconds<=end)return from+(to-from)*(seconds-start)/(end-start);
 }
 return 32;
}
async function render(page,scenario,tmp){
 const dir=fs.mkdtempSync(path.join(tmp,scenario.name+'-'));
 // A hash-only change would not reload the page, so start each scenario fresh.
 await page.goto('about:blank');
 await page.goto(pathToFileURL(path.join(root,'assets/src/pr-multi-target-demo.html')).href+'#'+scenario.name);
 await page.evaluate(()=>Promise.all([...document.images].map(i=>i.decode())));
 // A narrowed apply never merges, so its recording ends on the blocked check.
 const duration=introSeconds+(scenario.name==='target'?21.5:25.5);
 const frames=[];let lastCamera;
 for(let i=0;i<=duration*fps;i++){
  // Hold the settled final view without repainting identical DOM each frame.
  if(i>(duration-holdSeconds)*fps){frames.push(frames[frames.length-1]);continue;}
  const t=sceneTime(i/fps);
  await page.evaluate(({t,dt})=>window.renderFrame(t,dt),{t,dt:1/fps});
  const camera=await page.evaluate(()=>previousScroll);
  if(t>=7&&lastCamera!==undefined&&(camera<lastCamera-1||camera-lastCamera>26))throw new Error(scenario.name+': scrolling must move forward without jumps');
  lastCamera=camera;
  if(i===0&&!(await page.locator('.code.added').allTextContents()).join(' ').includes('idx_status'))throw new Error(scenario.name+': opening frame must show the index diff');
  if(i===introSeconds*fps){
   const checks=await page.locator('#checks').innerText();
   if(!checks.includes('1 apply pending')||!checks.includes('Merging is blocked'))throw new Error(scenario.name+': initial schema check must block merge');
  }
  if(i===(introSeconds+11)*fps){
   const text=await page.locator('#progress-body').innerText();
   for(const want of scenario.progress)if(!text.includes(want))throw new Error(`${scenario.name}: progress frame must show ${want}`);
  }
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  const frame=path.join(dir,`${String(i).padStart(4,'0')}.png`);await page.screenshot({path:frame});frames.push(frame);
 }
 const finalText=await page.locator('#checks').innerText();
 for(const want of scenario.final)if(!finalText.includes(want))throw new Error(`${scenario.name}: final checks must show ${want}`);
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
