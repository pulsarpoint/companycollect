import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import { formatDistanceToNow, format } from "date-fns";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDate(iso: string): string {
  return format(new Date(iso), "MMM d, yyyy HH:mm");
}

export function timeAgo(iso: string): string {
  return formatDistanceToNow(new Date(iso), { addSuffix: true });
}

export function signalColor(signal: string): string {
  const map: Record<string, string> = {
    registry_website: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
    wikidata: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
    certsh: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
    whois: "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200",
    search: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-200",
  };
  return map[signal] ?? map.search!;
}

export function confidenceColor(confidence: number): string {
  if (confidence >= 85) return "text-green-700 dark:text-green-400";
  if (confidence >= 65) return "text-blue-700 dark:text-blue-400";
  return "text-yellow-700 dark:text-yellow-400";
}
