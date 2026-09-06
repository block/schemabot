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
  const page=await browser.newPage({viewport:{width:960,height:780},deviceScaleFactor:1});
  await page.goto(pathToFileURL(path.join(root,'assets/src/pr-workflow-demo.html')).href);
  await page.evaluate(()=>Promise.all([...document.images].map(i=>i.decode())));
  const frames=[];
  for(let i=0;i<=216;i++){
   await page.evaluate(t=>window.renderFrame(t),i/8);
   const frame=path.join(tmp,`${String(i).padStart(4,'0')}.png`);await page.screenshot({path:frame});frames.push(frame);
  }
  execFileSync('magick',['-delay','12','-loop','0',...frames,'-layers','Optimize',path.join(root,'assets/pr-workflow-demo.gif')]);
  console.log('Rendered 217 frames, including 100% progress and merge');
 } finally {if(browser)await browser.close();fs.rmSync(tmp,{recursive:true,force:true})}
})().catch(e=>{console.error(e);process.exitCode=1});
