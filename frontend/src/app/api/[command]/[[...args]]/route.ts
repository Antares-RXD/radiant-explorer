import { NextResponse } from 'next/server';
import prisma from '@/lib/db';

// We removed rpcCall import. Everything must come from DB or external APIs (e.g. coingecko)

export async function GET(
    request: Request,
    { params }: { params: Promise<{ command: string; args?: string[] }> }
) {
    const { command, args: rawArgs } = await params;
    const args = rawArgs || [];
    const url = new URL(request.url);
    const queryParams = Object.fromEntries(url.searchParams);

    try {
        switch (command) {
            // --- Standard Commands (Now via DB) ---
            case 'getdifficulty': {
                const stats = await prisma.networkStats.findUnique({ where: { id: 1 } });
                const diff = stats?.difficulty || 0;
                return new NextResponse(diff.toString(), { headers: { 'Content-Type': 'text/plain' } });
            }
            case 'getconnectioncount': {
                const stats = await prisma.networkStats.findUnique({ where: { id: 1 } });
                const count = stats?.connections || 0;
                return new NextResponse(count.toString(), { headers: { 'Content-Type': 'text/plain' } });
            }
            case 'getblockcount': {
                const stats = await prisma.networkStats.findUnique({ where: { id: 1 } });
                let count = stats?.height || 0;
                if (!count) {
                    const maxBlock = await prisma.block.aggregate({ _max: { height: true } });
                    count = maxBlock._max.height || 0;
                }
                return new NextResponse(count.toString(), { headers: { 'Content-Type': 'text/plain' } });
            }
            case 'getblockhash': {
                const index = queryParams.index || args[0];
                if (!index) return NextResponse.json({ error: 'Missing index' }, { status: 400 });
                const block = await prisma.block.findUnique({
                    where: { height: parseInt(index) },
                    select: { hash: true }
                });
                if (!block?.hash) return NextResponse.json({ error: 'Block not found' }, { status: 404 });
                return new NextResponse(block.hash, { headers: { 'Content-Type': 'text/plain' } });
            }
            case 'getblock': {
                const hash = queryParams.hash || args[0];
                if (!hash) return NextResponse.json({ error: 'Missing hash' }, { status: 400 });
                const block = await prisma.block.findUnique({
                    where: { hash },
                    select: { rawData: true }
                });
                if (!block?.rawData) return NextResponse.json({ error: 'Block not found' }, { status: 404 });
                return NextResponse.json(block.rawData);
            }
            case 'getrawtransaction': {
                const txid = queryParams.txid || args[0];
                const decrypt = queryParams.decrypt === '1' ? 1 : 0;
                if (!txid) return NextResponse.json({ error: 'Missing txid' }, { status: 400 });

                // 1. Check Mined Txs
                let tx = await prisma.transaction.findUnique({
                    where: { txid },
                    select: { rawData: true }
                });

                // 2. Check Mempool
                if (!tx) {
                    const mempoolTx = await prisma.mempool.findUnique({
                        where: { txid },
                        select: { rawData: true }
                    });
                    tx = mempoolTx;
                }

                if (!tx?.rawData) return NextResponse.json({ error: 'Transaction not found' }, { status: 404 });

                const txData = tx.rawData as any;
                if (decrypt === 0) {
                    if (txData.hex) {
                        return new NextResponse(txData.hex, { headers: { 'Content-Type': 'text/plain' } });
                    }
                    return NextResponse.json({ error: 'Raw hex not available' }, { status: 501 });
                }

                return NextResponse.json(txData);
            }
            case 'getnetworkhashps': {
                const stats = await prisma.networkStats.findUnique({ where: { id: 1 } });
                const hashrate = stats?.hashrate || 0;
                return new NextResponse(hashrate.toString(), { headers: { 'Content-Type': 'text/plain' } });
            }

            // --- Extended Commands (DB) ---
            case 'getmoneysupply': {
                const result = await prisma.address.aggregate({ _sum: { balance: true } });
                const supply = result._sum.balance || 0;
                return new NextResponse(supply.toString(), { headers: { 'Content-Type': 'text/plain' } });
            }
            case 'getdistribution': {
                const addresses = await prisma.address.findMany({
                    where: { balance: { gt: 0 } },
                    select: { balance: true }
                });
                const balances = addresses.map(r => Number(r.balance));
                const total = balances.reduce((a, b) => a + b, 0);
                const distribution = {
                    supply: total,
                    t_1_25: { percent: 0, total: 0, count: 0 },
                    t_26_50: { percent: 0, total: 0, count: 0 },
                    t_51_75: { percent: 0, total: 0, count: 0 },
                    t_76_100: { percent: 0, total: 0, count: 0 },
                    t_101_plus: { percent: 0, total: 0, count: 0 }
                };
                return NextResponse.json(distribution);
            }
            case 'getaddress': {
                const addr = args[0] || queryParams.address;
                if (!addr) return NextResponse.json({ error: 'Missing address' }, { status: 400 });
                const address = await prisma.address.findUnique({ where: { address: addr } });
                if (!address) return NextResponse.json({ error: 'Address not found' }, { status: 404 });
                return NextResponse.json({
                    address: address.address,
                    sent: Number(address.totalSent),
                    received: Number(address.totalReceived),
                    balance: Number(address.balance).toString(),
                    last_txs: []
                });
            }
            case 'getaddresstxs': {
                const addr = args[0];
                const start = parseInt(args[1] || '0');
                const length = parseInt(args[2] || '50');
                if (!addr) return NextResponse.json({ error: 'Missing address' }, { status: 400 });

                const txAddresses = await prisma.txAddress.findMany({
                    where: { address: addr },
                    orderBy: { time: 'desc' },
                    take: length,
                    skip: start,
                    include: { transaction: { select: { rawData: true } } }
                });

                return NextResponse.json(txAddresses.map(r => r.transaction.rawData));
            }
            case 'gettx': {
                const txid = args[0];
                if (!txid) return NextResponse.json({ error: 'Missing txid' }, { status: 400 });

                let tx = await prisma.transaction.findUnique({
                    where: { txid },
                    select: { rawData: true }
                });

                if (!tx) {
                    const mempoolTx = await prisma.mempool.findUnique({
                        where: { txid },
                        select: { rawData: true }
                    });
                    tx = mempoolTx;
                }

                if (tx?.rawData) return NextResponse.json(tx.rawData);
                return NextResponse.json({ error: "Transaction not found" }, { status: 404 });
            }
            case 'getbalance': {
                const addr = args[0];
                if (!addr) return new NextResponse('0', { headers: { 'Content-Type': 'text/plain' } });
                const address = await prisma.address.findUnique({
                    where: { address: addr },
                    select: { balance: true }
                });
                const balance = address?.balance || 0;
                return new NextResponse(Number(balance).toString(), { headers: { 'Content-Type': 'text/plain' } });
            }
            case 'getlasttxs': {
                const min = parseFloat(args[0] || '0');
                const start = parseInt(args[1] || '0');
                const length = Math.min(parseInt(args[2] || '100'), 100);

                const txs = await prisma.transaction.findMany({
                    where: { totalOutput: { gte: min } },
                    orderBy: { time: 'desc' },
                    take: length,
                    skip: start,
                    select: { rawData: true }
                });
                return NextResponse.json(txs.map(r => r.rawData));
            }
            case 'getcurrentprice': {
                const gecko = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=radiant&vs_currencies=usd,btc');
                const data = await gecko.json();
                return NextResponse.json({ last_price_btc: data.neurai?.btc || 0, last_price_usd: data.neurai?.usd || 0 });
            }
            case 'getbasicstats':
            case 'getsummary': {
                const [stats, supplyResult, priceRes] = await Promise.all([
                    prisma.networkStats.findUnique({ where: { id: 1 } }),
                    prisma.address.aggregate({ _sum: { balance: true } }),
                    fetch('https://api.coingecko.com/api/v3/simple/price?ids=radiant&vs_currencies=usd,btc').then(r => r.json()).catch(() => ({}))
                ]);

                const supply = Number(supplyResult._sum.balance || 0);

                return NextResponse.json({
                    blockcount: stats?.height || 0,
                    difficulty: Number(stats?.difficulty || 0),
                    networkGraph: Number(stats?.hashrate || 0),
                    supply: supply,
                    connections: stats?.connections || 0,
                    price_btc: priceRes.neurai?.btc || 0,
                    price_usd: priceRes.neurai?.usd || 0
                });
            }
            default:
                return NextResponse.json({ error: `Unknown command: ${command}` }, { status: 404 });
        }

    } catch (error: any) {
        console.error(`API Error [${command}]:`, error);
        return NextResponse.json({ error: error.message || 'Internal Server Error' }, { status: 500 });
    }
}
