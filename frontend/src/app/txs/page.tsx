'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { queryKeys } from '@/lib/queryKeys';
import { formatDate, getAmountClass } from '@/lib/utils';
import { Card } from '@/components/ui/Card';
import { PageTitle } from '@/components/ui/PageTitle';
import { PaginationControls } from '@/components/ui/PaginationControls';
import { TxIdDisplay } from '@/components/TxIdDisplay';
import Link from 'next/link';
import { Transaction, TransactionOutput } from "@/types";
import config from '../../config.json';

function getTotalOutput(tx: Transaction) {
    if (typeof tx.totalOutput === 'number') {
        return tx.totalOutput;
    }
    return tx.vout.reduce((sum, v) => sum + (parseFloat(v.value) || 0), 0);
}

export default function TransactionsPage() {
    const [skip, setSkip] = useState(0);
    const limit = config.ui.pagination?.defaultLimit ?? 20;
    const minTotalOutput = config.thresholds?.amountColors?.low ?? 1_000_000;

    const { data: txs, isLoading } = useQuery({
        queryKey: queryKeys.transactions.list(skip, minTotalOutput),
        queryFn: () => api.getLatestTxs(limit, skip, minTotalOutput),
        refetchInterval: config.ui.pollingInterval,
        staleTime: config.ui.pollingInterval / 2,
    });

    return (
        <div className="flex flex-col gap-8 w-full">
            <PageTitle>Latest Transactions</PageTitle>

            <Card className="bg-card text-card-foreground border-border">
                <div className="flex flex-col">
                    {isLoading && <div className="p-8 text-center text-muted-foreground">Loading transactions...</div>}

                    {!isLoading && txs && (
                        <>
                            <div className="flex flex-col lg:hidden">
                                {txs.map((tx: Transaction) => {
                                    const totalOutput = getTotalOutput(tx);

                                    return (
                                        <div key={tx.txid} className="flex flex-col gap-2 py-3 border-b border-border last:border-0 hover:bg-muted/50 px-4 transition-colors">
                                            <Link
                                                href={`/tx/${tx.txid}`}
                                                className="block w-full min-w-0 overflow-hidden font-mono text-muted-foreground hover:text-foreground hover:underline"
                                                title={tx.txid}
                                            >
                                                <TxIdDisplay txid={tx.txid} className="text-sm" />
                                            </Link>
                                            <div className={`rounded-md px-2 py-1 text-center text-sm font-bold ${getAmountClass(totalOutput)}`}>
                                                {totalOutput ? totalOutput.toFixed(2) : '0.00'} RXD
                                            </div>
                                            <div className="flex items-center justify-between text-sm text-muted-foreground">
                                                <Link href={`/block/${tx.height}`} className="text-primary hover:underline font-bold">
                                                    #{tx.height}
                                                </Link>
                                                <span className="whitespace-nowrap">{formatDate(tx.blocktime)}</span>
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>

                            <div className="hidden lg:flex flex-col">
                                <div className="grid grid-cols-5 gap-4 px-6 py-3 text-sm text-muted-foreground uppercase bg-muted/50 border-b border-border">
                                    <div>Block</div>
                                    <div>Recipients</div>
                                    <div>TX</div>
                                    <div className="text-right">Amount</div>
                                    <div className="text-right">Time</div>
                                </div>
                                {txs.map((tx: Transaction) => {
                                    const totalOutput = getTotalOutput(tx);
                                    const recipients = tx.vout.filter((v: TransactionOutput) => v.scriptPubKey?.addresses?.length).length;

                                    return (
                                        <div key={tx.txid} className="grid grid-cols-5 items-center gap-4 py-3 border-b border-border last:border-0 hover:bg-muted/50 px-6 transition-colors">
                                            <Link href={`/block/${tx.height}`} className="font-bold text-primary hover:underline">
                                                #{tx.height}
                                            </Link>
                                            <div>{recipients}</div>
                                            <Link href={`/tx/${tx.txid}`} className="block min-w-0 font-mono text-primary hover:underline">
                                                <TxIdDisplay txid={tx.txid} className="text-base" />
                                            </Link>
                                            <div className={`justify-self-end rounded-md px-2 py-1 text-right font-bold ${getAmountClass(totalOutput)}`}>
                                                {totalOutput ? totalOutput.toFixed(2) : '0.00'} RXD
                                            </div>
                                            <div className="justify-self-end whitespace-nowrap text-sm text-muted-foreground">{formatDate(tx.blocktime)}</div>
                                        </div>
                                    );
                                })}
                            </div>

                            <PaginationControls
                                skip={skip}
                                limit={limit}
                                itemCount={txs.length}
                                itemName="Transactions"
                                onPrevious={() => setSkip(Math.max(0, skip - limit))}
                                onNext={() => setSkip(skip + limit)}
                            />
                        </>
                    )}
                </div>
            </Card>
        </div>
    );
}
