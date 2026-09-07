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
  const frames=[],fps=20,introSeconds=2.5,duration=17+introSeconds;
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
   if(i===0){
    if(!(await page.locator('#diff-view').isVisible())||!(await page.locator('.added').innerText()).includes('idx_email_created'))throw new Error('Opening frame must show the index diff');
   }
   if(i===introSeconds*fps){
    const checks=await page.locator('#checks').innerText();
    if(!checks.includes('Action required')||!checks.includes('Merging is blocked'))throw new Error('Initial schema check must block merge');
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
  execFileSync('magick',[...frames.filter((_,i)=>i%fps===0),'-append','-colors','256','-unique-colors',palette]);
  execFileSync('magick',['-delay','5','-loop','0',...frames,'+dither','-remap',palette,'-layers','Optimize',path.join(root,'assets/pr-workflow-demo.gif')]);
  const finalHashes=execFileSync('magick',[path.join(root,'assets/pr-workflow-demo.gif'),'-coalesce','-delete',`0-${(15+introSeconds)*fps-1}`,'-format','%#\n','info:'],{encoding:'utf8'}).trim().split('\n');
  if(new Set(finalHashes).size!==1)throw new Error('Final GIF reading pause must stay still');
  console.log('Rendered 391 frames: approximately 19.5 seconds, including the file diff, 100% progress and merge');
 } finally {if(browser)await browser.close();fs.rmSync(tmp,{recursive:true,force:true})}
})().catch(e=>{console.error(e);process.exitCode=1});
