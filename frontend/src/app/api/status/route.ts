import { NextResponse } from 'next/server';
import prisma from '@/lib/db';

export async function GET() {
    try {
        // Fetch network stats and latest block time in parallel
        const [stats, lastBlock] = await Promise.all([
            prisma.networkStats.findFirst(),
            prisma.block.findFirst({
                orderBy: { height: 'desc' },
                select: { time: true, height: true },
            }),
        ]);

        const lastBlockTime = lastBlock?.time || 0;
        const explorerHeight = lastBlock?.height || 0;

        // Stale check: if stats haven't been updated in 5 minutes, Node/Syncer might be down
        const now = Math.floor(Date.now() / 1000);
        const statsAge = now - (stats?.updatedAt || 0);
        const isStale = statsAge > 300; // 5 minutes

        // If stale, reports 0 connections to force Red status or similar logic in frontend
        const connectionCount = isStale ? 0 : (stats?.connections ?? 0);
        const nodeHeight = isStale ? 0 : (stats?.height ?? 0);


        // Build response matching SystemInfo interface
        const systemInfo = {
            blockbook: {
                coin: "Neurai",
                host: "custom-stack",
                version: "0.1.0",
                gitCommit: "custom",
                buildTime: new Date().toISOString(),
                syncMode: true,
                initialSync: false,
                inSync: true,
                bestHeight: explorerHeight,
                lastBlockTime: new Date(lastBlockTime * 1000).toISOString(),
                inSyncMempool: true,
                lastMempoolTime: new Date().toISOString(),
                mempoolSize: 0,
                decimals: 8,
                dbSize: 0,
                about: "Custom Radiant Explorer"
            },
            backend: {
                chain: "main",
                blocks: nodeHeight, // Use stale-aware height
                headers: nodeHeight,
                bestBlockHash: "",
                difficulty: stats?.difficulty?.toString() ?? "0",
                sizeOnDisk: 0,
                version: "1.0.0",
                subversion: "/Neurai:1.0.0/",
                protocolVersion: "70015",
                hashrate: Number(stats?.hashrate ?? 0),
                supply: Number(stats?.supply ?? 0),
                marketCap: Number(stats?.marketCapUsd ?? 0),
                price: Number(stats?.priceUsd ?? 0)
            }
        };

        return NextResponse.json(systemInfo);
    } catch (error) {
        console.error('Status API Error:', error);
        return NextResponse.json({ error: 'Internal Server Error' }, { status: 500 });
    }
}
