import { useState } from 'react';
import { Input } from '@/components/ui/input';
import { PasswordInput } from "@/components/ui/password-input";
import { Separator } from '@/components/ui/separator';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';

export function Settings() {
    const [values, setValues] = useState({
        SQLITE_SOURCE: window.env.SQLITE_SOURCE || '',
        NOTION_API: window.env.NOTION_API_KEY || '',
        NOTION_DB: window.env.NOTION_DATABASE_ID || '',
        GEMINI_API: window.env.GEMINI_API_KEY || ''
    });
    const [isSaving, setIsSaving] = useState(false);

    const handleChange = (key: string, value: string) => {
        setValues(prev => ({ ...prev, [key]: value }));
    };

    const handleSave = async () => {
        setIsSaving(true);
        try {
            const entries = Object.entries(values).map(([key, value]) => ({ key, value }));
            await window.api.updateEnvValue(entries);
            toast.success('Settings saved successfully');
        } catch (error) {
            toast.error('Failed to save settings');
            console.error('Error saving settings:', error);
        } finally {
            setIsSaving(false);
        }
    };

    return (
        <>
            <div className="flex justify-between items-center p-4 pb-0">
                <h1 className="text-2xl font-bold">Settings</h1>
                <Button onClick={handleSave} disabled={isSaving}>
                    {isSaving ? 'Saving...' : 'Save Changes'}
                </Button>
            </div>
            <div className="p-4 flex justify-center">
                <div className="grid gap-6 w-1/3">
                    <div className="space-y-2">
                        <label className="text-md font-medium">Kobo Highlights File Path</label>
                        <Input
                            type="text"
                            value={values.SQLITE_SOURCE}
                            onChange={(e) => handleChange('SQLITE_SOURCE', e.target.value)}
                        />
                    </div>
                    <Separator />
                    <div className="space-y-2">
                        <label className="text-md font-medium">Notion API Key</label>
                        <PasswordInput
                            value={values.NOTION_API}
                            onChange={(e) => handleChange('NOTION_API', e.target.value)}
                        />
                    </div>
                    <div className="space-y-2">
                        <label className="text-md font-medium">Notion Database ID</label>
                        <PasswordInput
                            value={values.NOTION_DB}
                            onChange={(e) => handleChange('NOTION_DB', e.target.value)}
                        />
                    </div>
                    <Separator />
                    <div className="space-y-2">
                        <label className="text-md font-medium">Gemini API Key</label>
                        <PasswordInput
                            value={values.GEMINI_API}
                            onChange={(e) => handleChange('GEMINI_API', e.target.value)}
                        />
                    </div>
                </div>
            </div>
        </>
    );
} 