#!/usr/bin/env node
// Render the illustrative PR timeline with one reusable browser session.
// Requires Playwright and ImageMagick. The original pr-demo.gif is preserved.
// Pass "postgres" or "vitess" to record that workflow instead of MySQL.
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {execFileSync}=require('node:child_process');
const {pathToFileURL}=require('node:url');
const {chromium}=require('playwright');
const engine=process.argv[2]||'mysql';
// timing maps real recording seconds to scene seconds, so reading pauses stay
// and the copy compresses. opening is a token the first frame's added diff
// lines must carry. hold is the real second after which the settled merge view
// repeats instead of repainting.
const SQL_TIMING=[[0,0],[2,3],[4,7],[5,9],[6.5,10],[9.5,20],[10.5,21.5],[11.5,23],[13.5,25],[15.5,29],[18.5,32]];
const ENGINES={
 // diffColors are the opening diff's deleted and added line backgrounds.
 mysql:{gif:'assets/pr-workflow-demo.gif',done:'100.00%',opening:'idx_email_created',diffColors:['FFEBE9','FFCECB','DAFBE1','ACEEBB'],timing:SQL_TIMING,hold:16.5},
 // The PostgreSQL diff only adds lines.
 postgres:{gif:'assets/pr-postgres-workflow-demo.gif',done:'95.00%',opening:'idx_email_created',diffColors:['DAFBE1','ACEEBB'],timing:SQL_TIMING,hold:16.5},
 // Vitess runs longer: the copy is followed by a cutover and an open revert
 // window before the apply completes and the merge unblocks.
 vitess:{gif:'assets/pr-vitess-workflow-demo.gif',done:'100.00%',opening:'note',diffColors:['DAFBE1','ACEEBB'],
  timing:[[0,0],[2,3],[4,7],[5,9],[6.5,10],[9.5,20],[10.5,21.5],[13,24],[14,25.5],[16,27.5],[18,31.5],[21,34.5]],hold:19},
};
if(!ENGINES[engine])throw new Error(`Unknown engine ${engine}; expected one of ${Object.keys(ENGINES).join(', ')}`);
(async()=>{
 const root=path.resolve(__dirname,'..'),gif=path.join(root,ENGINES[engine].gif),tmp=fs.mkdtempSync(path.join(os.tmpdir(),'pr-demo-'));
 const chrome=process.env.CHROME||'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
 let browser;
 try {
  browser=await chromium.launch({headless:true,...(fs.existsSync(chrome)?{executablePath:chrome}:{})});
  const page=await browser.newPage({viewport:{width:1100,height:820},deviceScaleFactor:1});
  const url=pathToFileURL(path.join(root,'assets/src/pr-workflow-demo.html'));url.search=`engine=${engine}`;
  await page.goto(url.href);
  await page.evaluate(()=>Promise.all([...document.images].map(i=>i.decode())));
  // Keep reading pauses, but compress copying to three seconds.
  const timing=ENGINES[engine].timing,sceneEnd=timing[timing.length-1][1];
  const frames=[],fps=20,introSeconds=4,duration=timing[timing.length-1][0]+introSeconds;
  let lastCamera;
  function sceneTime(seconds){
   if(seconds<introSeconds)return -1;
   seconds-=introSeconds;
   for(let j=1;j<timing.length;j++){
    const [end,to]=timing[j], [start,from]=timing[j-1];
    if(seconds<=end)return from+(to-from)*(seconds-start)/(end-start);
   }
   return sceneEnd;
  }
  for(let i=0;i<=duration*fps;i++){
   // Hold the settled merge view without repainting identical DOM each frame.
   if(i>(ENGINES[engine].hold+introSeconds)*fps){frames.push(frames[frames.length-1]);continue;}
   await page.evaluate(({t,dt})=>window.renderFrame(t,dt),{t:sceneTime(i/fps),dt:1/fps});
   const camera=await page.evaluate(()=>previousScroll);
   const t=sceneTime(i/fps);
   if(t>=7&&lastCamera!==undefined){
    if(camera<lastCamera-1||camera-lastCamera>26)throw new Error('Workflow scrolling must move forward without jumps');
   }
   lastCamera=camera;
   if(i===0){
    if(!(await page.locator('#diff-view').isVisible())||!(await page.locator('.code.added').allTextContents()).join(' ').includes(ENGINES[engine].opening))throw new Error(`Opening frame must show the ${ENGINES[engine].opening} diff`);
   }
   if(i===introSeconds*fps){
    const checks=await page.locator('#checks').innerText();
    if(!checks.includes('1 apply pending')||!checks.includes('Merging is blocked'))throw new Error('Initial schema check must block merge');
   }
   if(i===(9.5+introSeconds)*fps){
    const text=await page.locator('#progress-body').innerText();
    if(!text.includes(ENGINES[engine].done)||text.includes('DEMOVALUE'))throw new Error(`Progress frame must show ${ENGINES[engine].done} with no placeholder text`);
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
  execFileSync('magick',['-delay','5','-loop','0',...frames,'+dither','-remap',palette,'-layers','OptimizePlus',gif]);
  const openingColors=execFileSync('magick',[gif+'[0]','-format','%c','histogram:info:'],{encoding:'utf8'});
  for(const hex of ENGINES[engine].diffColors)if(!openingColors.includes('#'+hex))throw new Error('GIF must preserve diff color #'+hex);
  // Cropping also combines identical frames, so validate the hold by duration.
  const timeline=execFileSync('magick',[gif,'-coalesce','-format','%T %#\n','info:'],{encoding:'utf8'}).trim().split('\n').map(line=>line.split(' '));
  let held=0;const finalHash=timeline[timeline.length-1][1];
  for(const [delay,hash] of timeline.reverse()){
   if(held>=200)break;
   if(hash!==finalHash)throw new Error('Final GIF reading pause must stay still');
   held+=Number(delay);
  }
  if(held<200)throw new Error('Final GIF reading pause must last two seconds');
  console.log(`Rendered ${frames.length} frames: approximately ${duration.toFixed(1)} seconds, including the file diff, ${ENGINES[engine].done} progress and merge`);
 } finally {if(browser)await browser.close();fs.rmSync(tmp,{recursive:true,force:true})}
})().catch(e=>{console.error(e);process.exitCode=1});
