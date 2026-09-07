#!/usr/bin/env node
// Render the illustrative PR timeline with one reusable browser session.
// Requires Playwright and ImageMagick. The original pr-demo.gif is preserved.
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {execFileSync}=require('node:child_process');
const {pathToFileURL}=require('node:url');
const {chromium}=require('playwright');
(async()=>{
 const root=path.resolve(__dirname,'..'),tmp=fs.mkdtempSync(path.join(os.tmpdir(),'pr-demo-'));
 const chrome=process.env.CHROME||'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
 let browser;
 try {
  browser=await chromium.launch({headless:true,...(fs.existsSync(chrome)?{executablePath:chrome}:{})});
  const page=await browser.newPage({viewport:{width:1100,height:820},deviceScaleFactor:1});
  await page.goto(pathToFileURL(path.join(root,'assets/src/pr-workflow-demo.html')).href);
  await page.evaluate(()=>Promise.all([...document.images].map(i=>i.decode())));
  const frames=[],fps=20,introSeconds=4,duration=17+introSeconds;
  let lastCamera;
  // Keep reading pauses, but compress copying to three seconds.
  const timing=[[0,0],[2,3],[4,7],[5,10],[8,20],[9,21.5],[10,23],[12,25],[14,29],[17,32]];
  function sceneTime(seconds){
   if(seconds<introSeconds)return -1;
   seconds-=introSeconds;
   for(let j=1;j<timing.length;j++){
    const [end,to]=timing[j], [start,from]=timing[j-1];
    if(seconds<=end)return from+(to-from)*(seconds-start)/(end-start);
   }
   return 32;
  }
  for(let i=0;i<=duration*fps;i++){
   // Hold the settled merge view without repainting identical DOM each frame.
   if(i>(15+introSeconds)*fps){frames.push(frames[frames.length-1]);continue;}
   await page.evaluate(({t,dt})=>window.renderFrame(t,dt),{t:sceneTime(i/fps),dt:1/fps});
   const camera=await page.evaluate(()=>previousScroll);
   const t=sceneTime(i/fps);
   if(t>=7&&lastCamera!==undefined){
    if(camera<lastCamera-1||camera-lastCamera>26)throw new Error('Workflow scrolling must move forward without jumps');
   }
   lastCamera=camera;
   if(i===0){
    if(!(await page.locator('#diff-view').isVisible())||!(await page.locator('.code.added').allTextContents()).join(' ').includes('idx_email_created'))throw new Error('Opening frame must show the index diff');
   }
   if(i===introSeconds*fps){
    const checks=await page.locator('#checks').innerText();
    if(!checks.includes('1 apply pending')||!checks.includes('Merging is blocked'))throw new Error('Initial schema check must block merge');
   }
   if(i===(8+introSeconds)*fps){
    const text=await page.locator('#progress-body').innerText();
    if(!text.includes('100.00%')||text.includes('DEMOVALUE'))throw new Error('Progress frame must show 100% with no placeholder text');
   }
   await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
   const frame=path.join(tmp,`${String(i).padStart(4,'0')}.png`);await page.screenshot({path:frame});frames.push(frame);
  }
  if(!(await page.locator('#checks').innerText()).includes('All checks have passed'))throw new Error('Final checks must pass');
  // A shared palette keeps text and neutral backgrounds from shimmering between frames.
  const palette=path.join(tmp,'palette.png');
  // Keep GitHub's diff and UI colors exact; frequency-based quantization alone
  // blends the brief opening's red/green backgrounds into the neutral palette.
  const fixedColors=['ffffff','f6f8fa','ffebe9','ffcecb','dafbe1','aceebb','ddf4ff','d1d9e0','1f2328','59636e','cf222e','1a7f37','0969da','8250df','fbefff','9a6700'];
  const swatches=path.join(tmp,'fixed.ppm'),adaptive=path.join(tmp,'adaptive.png');
  fs.writeFileSync(swatches,`P3\n${fixedColors.length} 1\n255\n`+fixedColors.map(hex=>hex.match(/../g).map(v=>parseInt(v,16)).join(' ')).join('\n'));
  execFileSync('magick',[...frames.filter((_,i)=>i%fps===0),'-append','-colors',String(256-fixedColors.length),'-unique-colors',adaptive]);
  execFileSync('magick',[adaptive,swatches,'+append','-unique-colors',palette]);
  // OptimizePlus preserves colors and combines still frames with their delays.
  execFileSync('magick',['-delay','5','-loop','0',...frames,'+dither','-remap',palette,'-layers','OptimizePlus',path.join(root,'assets/pr-workflow-demo.gif')]);
  const openingColors=execFileSync('magick',[path.join(root,'assets/pr-workflow-demo.gif')+'[0]','-format','%c','histogram:info:'],{encoding:'utf8'});
  for(const hex of ['FFEBE9','FFCECB','DAFBE1','ACEEBB'])if(!openingColors.includes('#'+hex))throw new Error('GIF must preserve diff color #'+hex);
  // Cropping also combines identical frames, so validate the hold by duration.
  const timeline=execFileSync('magick',[path.join(root,'assets/pr-workflow-demo.gif'),'-coalesce','-format','%T %#\n','info:'],{encoding:'utf8'}).trim().split('\n').map(line=>line.split(' '));
  let held=0;const finalHash=timeline[timeline.length-1][1];
  for(const [delay,hash] of timeline.reverse()){
   if(held>=200)break;
   if(hash!==finalHash)throw new Error('Final GIF reading pause must stay still');
   held+=Number(delay);
  }
  if(held<200)throw new Error('Final GIF reading pause must last two seconds');
  console.log('Rendered 421 frames: approximately 21 seconds, including the file diff, 100% progress and merge');
 } finally {if(browser)await browser.close();fs.rmSync(tmp,{recursive:true,force:true})}
})().catch(e=>{console.error(e);process.exitCode=1});
