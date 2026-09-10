// NODE_PATH must resolve Playwright. Requires Chrome and ImageMagick on PATH.
const fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const {execFileSync}=require('node:child_process');
const {chromium}=require('playwright');
(async()=>{
const root=path.resolve(__dirname,'..'),out=path.join(root,'assets');
const framesDir=fs.mkdtempSync(path.join(os.tmpdir(),'spirit-keys-'));
const browser=await chromium.launch({headless:true,...(process.env.CHROME ? {executablePath:process.env.CHROME} : {channel:'chrome'})});
try{
 const page=await browser.newPage({viewport:{width:1100,height:690},deviceScaleFactor:1});
 page.on('pageerror',e=>console.error(e));
 await page.setContent(fs.readFileSync(path.join(root,'assets/src/spirit-primary-keys.html'),'utf8'));await page.evaluate(()=>document.fonts.ready);
 const frames=[],fps=20,duration=26;
 for(let i=0;i<fps*duration;i++){
  if(i>495){frames.push(frames[frames.length-1]);continue;}
  const f=path.join(framesDir,String(i).padStart(4,'0')+'.png');
  await page.evaluate(t=>renderFrame(t),i/fps);await page.screenshot({path:f});frames.push(f);
  if(i%100===0)console.log('Rendered frame '+i+'/'+fps*duration);
 }
 const colors=['ffffff','f6f8fa','d1d9e0','1f2328','59636e','0969da','ddf4ff','1a7f37','dafbe1','9a6700','fff8c5','8250df','fbefff'];
 const swatches=path.join(framesDir,'fixed.ppm'),palette=path.join(framesDir,'palette.png'),adaptive=path.join(framesDir,'adaptive.png');
 fs.writeFileSync(swatches,'P3\n'+colors.length+' 1\n255\n'+colors.map(h=>h.match(/../g).map(v=>parseInt(v,16)).join(' ')).join('\n'));
 execFileSync('magick',[...frames.filter((_,i)=>i%20===0),'-append','-colors',String(256-colors.length),'-unique-colors',adaptive]);
 execFileSync('magick',[adaptive,swatches,'+append','-unique-colors',palette]);
 console.log('Encoding shared-palette GIF');
 execFileSync('magick',['-limit','memory','512MiB','-delay','5','-loop','0',...frames,'+dither','-remap',palette,'-layers','OptimizePlus',path.join(out,'spirit-primary-keys.gif')]);
 console.log('GIF ready: '+fs.statSync(path.join(out,'spirit-primary-keys.gif')).size+' bytes');
}finally{await browser.close();fs.rmSync(framesDir,{recursive:true,force:true});}
})().catch(e=>{console.error(e);process.exitCode=1});
