import { useNavigate } from "react-router";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";

const destinations = { servers: "/admin/browsers", profiles: "/admin/browsers/profiles", testing: "/admin/browsers/testing", settings: "/admin/browsers/settings" };

export function BrowserTabs({ value }: { value: keyof typeof destinations }) {
  const navigate = useNavigate();
  return <Tabs value={value} onValueChange={value => navigate(destinations[value as keyof typeof destinations])}>
    <TabsList aria-label="Browser views">
      <TabsTrigger value="servers">Servers &amp; sessions</TabsTrigger>
      <TabsTrigger value="profiles">Saved sessions</TabsTrigger>
      <TabsTrigger value="testing">Testing</TabsTrigger>
      <TabsTrigger value="settings">Settings</TabsTrigger>
    </TabsList>
  </Tabs>;
}
