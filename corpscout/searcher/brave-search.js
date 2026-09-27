const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({
    headless: false
  });
  const context = await browser.newContext({
      permissions: ['clipboard-read', 'clipboard-write'],
  });
  const page = await context.newPage();
  await page.goto('https://search.brave.com/');
  await page.getByTestId('searchbox').click();
  await page.getByTestId('searchbox').click();
  await page.getByTestId('searchbox').fill('Can you give me more information about sweden company +1 Kommunikationsbyrå AB\n');
  await page.goto('https://search.brave.com/search?q=Can+you+give+me+more+information+about+sweden+company+%2B1+Kommunikationsbyr%C3%A5+AB%0D%0A&source=web&conversation=097215a0cb8ca41858d3420194e7a5d7b306');
  await page.getByRole('button', { name: 'More' }).click();
  await page.getByRole('button', { name: 'Copy' }).click();

  const copiedText = await page.evaluate(() =>
    navigator.clipboard.readText()
  );

  console.log('Copied text:');
  console.log(copiedText);
  await page.close();

  // ---------------------
  await context.close();
  await browser.close();
})();