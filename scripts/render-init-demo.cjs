#!/usr/bin/env node
// Render the recorded wizard and first plan with a stable palette and reading pauses.
// Requires Playwright and ImageMagick; uses Chrome when installed, otherwise Playwright Chromium.
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {execFileSync}=require('node:child_process');
const {pathToFileURL}=require('node:url');
const {chromium}=require('playwright');
(async()=>{
 const root=path.resolve(__dirname,'..'),tmp=fs.mkdtempSync(path.join(os.tmpdir(),'init-gif-'));
 const recording=JSON.parse(fs.readFileSync(path.join(root,'assets/src/init-demo-recording.json')));
 if(!recording.wizard.includes('Baseline plan: no changes.')||!recording.plan.includes('ALTER'))throw new Error('Record a successful real wizard and plan first');
 const chrome=process.env.CHROME||'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
 const browser=await chromium.launch({headless:true,...(fs.existsSync(chrome)?{executablePath:chrome}:{})});
 try{
  const page=await browser.newPage({viewport:{width:1100,height:820},deviceScaleFactor:1});
  await page.goto(pathToFileURL(path.join(root,'assets/src/init-demo.html')).href);
  await page.evaluate(data=>window.setRecording(data),recording);
  const frames=[],fps=10;let previous;
  for(let i=0;i<=42*fps;i++){
   await page.evaluate(t=>window.renderFrame(t,.1),i/fps);
   const frame=path.join(tmp,String(i).padStart(4,'0')+'.png');
   const pixels=await page.screenshot();
   if(previous && pixels.equals(previous))frames[frames.length-1].delay+=10;
   else { fs.writeFileSync(frame,pixels);frames.push({path:frame,delay:10});previous=pixels; }
   if([30,85,130,160,190,250,330,400].includes(i))fs.writeFileSync(path.join(tmp,'preview-'+i+'.png'),pixels);
  }
  const palette=path.join(tmp,'palette.png');
  execFileSync('magick',[...frames.filter((_,i)=>i%5===0).map(f=>f.path),'-append','-colors','256','-unique-colors',palette]);
  const out=path.join(root,'assets/init-demo.gif');
  execFileSync('magick',['-loop','0',...frames.flatMap(f=>['-delay',String(f.delay),f.path]),'+dither','-remap',palette,'-layers','OptimizePlus',out]);
  console.log(out,fs.statSync(out).size,'bytes');console.log('Preview frames:',tmp);
 }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exitCode=1});
