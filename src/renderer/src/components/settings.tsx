import { Input } from '@/components/ui/input';
import { Separator } from '@/components/ui/separator';

export function Settings() {
    return (
        <div className="container mx-auto p-4">
            <h2 className="text-2xl font-bold mb-6">Settings</h2>
            <div className="max-w-2xl mx-auto grid gap-6">
                <div className="space-y-2">
                    <label className="text-sm font-medium">Kobo Highlights File Path</label>
                    <Input type="text" value={window.env.SQLITE_SOURCE} readOnly />
                </div>
                <Separator />
                <div className="space-y-2">
                    <label className="text-sm font-medium">Notion API Key</label>
                    <Input type="password" value={window.env.NOTION_API_KEY} readOnly />
                </div>
                <div className="space-y-2">
                    <label className="text-sm font-medium">Notion Database ID</label>
                    <Input type="password" value={window.env.NOTION_DATABASE_ID} readOnly />
                </div>
                <Separator />
                <div className="space-y-2">
                    <label className="text-sm font-medium">Gemini API Key</label>
                    <Input type="password" value={window.env.GEMINI_API_KEY} readOnly />
                </div>
            </div>
        </div>
    );
} 