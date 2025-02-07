import { Input } from '@/components/ui/input';
import { Separator } from '@/components/ui/separator';

export function Settings() {
    return (
        <>
            <div className="flex justify-between items-center p-4 pb-0">
                <h1 className="text-2xl font-bold">Settings</h1>
            </div>
            <div className="p-4 flex justify-center">
                <div className="grid gap-6 w-1/3">
                    <div className="space-y-2">
                        <label className="text-md font-medium">Kobo Highlights File Path</label>
                        <Input type="text" value={window.env.SQLITE_SOURCE} readOnly />
                    </div>
                    <Separator />
                    <div className="space-y-2">
                        <label className="text-md font-medium">Notion API Key</label>
                        <Input type="password" value={window.env.NOTION_API_KEY} readOnly />
                    </div>
                    <div className="space-y-2">
                        <label className="text-md font-medium">Notion Database ID</label>
                        <Input type="password" value={window.env.NOTION_DATABASE_ID} readOnly />
                    </div>
                    <Separator />
                    <div className="space-y-2">
                        <label className="text-md font-medium">Gemini API Key</label>
                        <Input type="password" value={window.env.GEMINI_API_KEY} readOnly />
                    </div>
                </div>
            </div>
        </>
    );
} 