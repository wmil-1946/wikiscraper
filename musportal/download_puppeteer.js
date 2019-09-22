const puppeteer = require('puppeteer');

let urls = [];
let gotUrls = false;

process.argv.forEach(function(arg) {
    if (arg == '--urls') {
        gotUrls = true;
    } else if (gotUrls) {
        urls.push(arg)
    }
});

console.log(`Downloading ${urls.length} urls...`);

urls.forEach(function(url) {
    (async () => {
        try {
      const browser = await puppeteer.launch({
          headless: false
      });
      const page = await browser.newPage();
      await page.evaluateOnNewDocument(() => {
          Object.defineProperty(navigator, 'webdriver', {
            get: () => false,
          });
        });
      await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36');
      console.log(`~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`);
      console.log(url);
      console.log(`~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`);
      await page.goto(url);
      await page.waitForSelector('#mainContent');
      var content = await page.content();
      console.log(content);
      console.log(`~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`);

      await browser.close();
      } catch (e) {
            process.exit(1)
        }
    })();
});
