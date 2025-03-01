import { IActivitiesStorage, IWorkflowStorage } from "task-system-package";
import { MaybePromise } from "task-system-package";
import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';

interface WorkflowData {
    args: any;
    result?: any;
    additionalData?: any;
    activities: {
        [activityId: string]: {
            providerName: string;
            activityName: string;
            args: any;
            result?: any;
            additionalData?: any;
        }
    }
}

export class AsJSONFilesTaskSystemStorage implements IWorkflowStorage<any>, IActivitiesStorage<any, any> {
    private basePath: string;

    constructor(storagePath: string = './storage') {
        this.basePath = storagePath;
        fsSync.mkdirSync(this.basePath, { recursive: true });
    }

    private getWorkflowPath(workflowname: string, workflowId: string): string {
        return path.join(this.basePath, `${workflowname}_${workflowId}.json`);
    }

    private async readWorkflowFile(workflowname: string, workflowId: string): Promise<WorkflowData | undefined> {
        try {
            const content = await fs.readFile(this.getWorkflowPath(workflowname, workflowId), 'utf-8');
            return JSON.parse(content);
        } catch (err) {
            return undefined;
        }
    }

    private async writeWorkflowFile(workflowname: string, workflowId: string, data: WorkflowData): Promise<void> {
        await fs.writeFile(
            this.getWorkflowPath(workflowname, workflowId),
            JSON.stringify(data, null, 2),
            'utf-8'
        );
    }

    async listWorkflowsIdsWithoutResult(): Promise<string[]> {
        const files = await fs.readdir(this.basePath);
        const results: string[] = [];

        for (const file of files) {
            try {
                const content = await fs.readFile(path.join(this.basePath, file), 'utf-8');
                const data = JSON.parse(content) as WorkflowData;
                if (!data.result) {
                    const parts = file.split('_');
                    const workflowId = parts[parts.length - 1];
                    results.push(workflowId.replace('.json', ''));
                }
            } catch (err) {
                console.error(`Error reading file ${file}:`, err);
            }
        }

        return results;
    }

    async getWorkflow(data: { workflowname: string; workflowId: string; return: (data: { args: any; result?: any; }) => MaybePromise<{ args: any; result?: any; } & { _brand: "return"; }>; }): Promise<({ args: any; result?: any; } & { _brand: "return"; }) | undefined> {
        const workflowData = await this.readWorkflowFile(data.workflowname, data.workflowId);
        if (!workflowData) return undefined;
        return data.return(workflowData);
    }

    async setWorkflow(data: { args: any; result?: any; workflowname: string; workflowId: string; }): Promise<void> {
        const existing = await this.readWorkflowFile(data.workflowname, data.workflowId) || { activities: {} };
        await this.writeWorkflowFile(data.workflowname, data.workflowId, {
            ...existing,
            args: data.args,
            result: data.result
        });
    }

    async getWorkflowAdditionalData(data: { workflowname: string; workflowId: string; return: (data: any) => any; }) {
        const workflowData = await this.readWorkflowFile(data.workflowname, data.workflowId);
        if (!workflowData?.additionalData) return undefined;
        return data.return(workflowData.additionalData);
    }

    async setWorkflowAdditionalData(data: { additionalData: any; workflowname: string; workflowId: string; }): Promise<void> {
        const existing = await this.readWorkflowFile(data.workflowname, data.workflowId) || { activities: {}, args: {} };
        await this.writeWorkflowFile(data.workflowname, data.workflowId, {
            ...existing,
            additionalData: data.additionalData
        });
    }

    async getActivity(data: { providerName: any; activityName: string; args: any; activityId: string; workflowName: string; workflowId: string; return: (data: any) => any; }) {
        const workflowData = await this.readWorkflowFile(data.workflowName, data.workflowId);
        const activity = workflowData?.activities[data.activityId];
        if (!activity) return undefined;
        return data.return(activity);
    }

    async setActivity(data: { result: any; args: any; activityname: string; providername: any; activityId: string; workflowName: string; workflowId: string; }): Promise<void> {
        const existing = await this.readWorkflowFile(data.workflowName, data.workflowId) || { activities: {}, args: {} };
        existing.activities[data.activityId] = {
            ...existing.activities[data.activityId],
            providerName: data.providername,
            activityName: data.activityname,
            args: data.args,
            result: data.result
        };
        await this.writeWorkflowFile(data.workflowName, data.workflowId, existing);
    }

    async getActivityAdditionalData(data: { activityname: string; providername: any; args: any; activityId: string; workflowName: string; workflowId: string; return: (data: any) => any; }) {
        const workflowData = await this.readWorkflowFile(data.workflowName, data.workflowId);
        const activity = workflowData?.activities[data.activityId];
        if (!activity?.additionalData) return undefined;
        return data.return(activity.additionalData);
    }

    async setActivityAdditionalData(data: { additionalData: any; activityname: string; providername: any; args: any; activityId: string; workflowName: string; workflowId: string; }): Promise<void> {
        const existing = await this.readWorkflowFile(data.workflowName, data.workflowId) || { activities: {}, args: {} };
        if (!existing.activities[data.activityId]) {
            existing.activities[data.activityId] = {
                providerName: data.providername,
                activityName: data.activityname,
                args: data.args,
                additionalData: data.additionalData
            };
        } else {
            existing.activities[data.activityId]!.additionalData = data.additionalData;
        }
        await this.writeWorkflowFile(data.workflowName, data.workflowId, existing);
    }
}
