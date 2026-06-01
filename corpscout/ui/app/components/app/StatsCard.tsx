import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import type { LucideIcon } from "lucide-react";

interface StatsCardProps {
  title: string;
  value: number | string;
  icon?: LucideIcon;
  variant?: "default" | "success" | "danger" | "warning";
  href?: string;
}

const variantClass: Record<NonNullable<StatsCardProps["variant"]>, string> = {
  default: "text-foreground",
  success: "text-green-600 dark:text-green-400",
  danger: "text-red-600 dark:text-red-400",
  warning: "text-yellow-600 dark:text-yellow-400",
};

export function StatsCard({ title, value, icon: Icon, variant = "default", href }: StatsCardProps) {
  const content = (
    <Card className={href ? "cursor-pointer hover:bg-muted/50 transition-colors" : undefined}>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
        {Icon && <Icon className="size-4 text-muted-foreground" />}
      </CardHeader>
      <CardContent>
        <p className={`text-2xl font-bold ${variantClass[variant]}`}>
          {typeof value === "number" ? value.toLocaleString() : value}
        </p>
      </CardContent>
    </Card>
  );

  return href ? <a href={href}>{content}</a> : content;
}
