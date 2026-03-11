import { type ClassValue, clsx } from "clsx";
import config from '../config.json';

export function cn(...inputs: ClassValue[]) {
  return clsx(inputs);
}

/**
 * Format a Unix timestamp to locale string
 */
export function formatDate(
  timestamp: number,
  options?: Intl.DateTimeFormatOptions
): string {
  const defaultOptions: Intl.DateTimeFormatOptions = {
    year: '2-digit',
    month: 'numeric',
    day: 'numeric',
    hour: 'numeric',
    minute: 'numeric',
    second: 'numeric',
  };
  return new Date(timestamp * 1000).toLocaleString(undefined, options ?? defaultOptions);
}

/**
 * Format a number with specified decimal places
 */
export function formatNumber(value: number, decimals = 2): string {
  return value.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

/**
 * Format satoshis to RXD with 8 decimal places
 */
export function formatRXD(satoshis: number | bigint): string {
  const value = typeof satoshis === 'bigint' ? Number(satoshis) : satoshis;
  return (value / 1e8).toFixed(8);
}

// Singleton formatters for performance
const currencyFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
});

const compactNumberFormatter = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 2,
});

/**
 * Format a number as USD currency
 */
export function formatCurrency(value: number): string {
  return currencyFormatter.format(value);
}

/**
 * Format a number in compact notation (e.g., 1.2K, 3.4M)
 */
export function formatCompactNumber(value: number): string {
  return compactNumberFormatter.format(value);
}

/**
 * Format hashrate with appropriate unit
 */
export function formatHashrate(rate: number): string {
  if (rate < 1e3) return `${rate.toFixed(2)} H/s`;
  if (rate < 1e6) return `${(rate / 1e3).toFixed(2)} KH/s`;
  if (rate < 1e9) return `${(rate / 1e6).toFixed(2)} MH/s`;
  if (rate < 1e12) return `${(rate / 1e9).toFixed(2)} GH/s`;
  if (rate < 1e15) return `${(rate / 1e12).toFixed(2)} TH/s`;
  return `${(rate / 1e15).toFixed(2)} PH/s`;
}

/**
 * Get CSS class for transaction amount highlighting based on thresholds
 */
export function getAmountClass(amount: number): string {
  const thresholds = config.thresholds?.amountColors ?? {
    high: 50_000_000,
    medium: 10_000_000,
    low: 1_000_000,
  };

  if (amount >= thresholds.high) {
    return "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-200";
  }
  if (amount >= thresholds.medium) {
    return "bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-200";
  }
  if (amount >= thresholds.low) {
    return "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-200";
  }
  return "bg-muted/40 text-foreground";
}

/**
 * Get total output from a transaction
 */
export function getTotalOutput(tx: { vout?: Array<{ value?: number }> }): number {
  if (!tx.vout) return 0;
  return tx.vout.reduce((sum, output) => sum + (output.value || 0), 0);
}
