import { test, expect } from '@playwright/test';

test('test', async ({ page }) => {
  await page.goto('https://search.brave.com/');
  await page.getByTestId('searchbox').click();
  await page.getByTestId('searchbox').click();
  await page.getByTestId('searchbox').fill('Can y');
  await page.getByTestId('searchbox').press('ControlOrMeta+o');
  await page.getByTestId('searchbox').fill('Can you give me more information about Anna Celay and +1 Kommunikationsbyrå AB');
  await page.getByRole('button', { name: 'Ask' }).click();
  await page.locator('button').filter({ hasText: 'Copy' }).click();
});