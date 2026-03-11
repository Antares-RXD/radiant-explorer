'use client';

import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, Block, SystemInfo } from '@/lib/api';
import { Card } from '@/components/ui/Card';
import { SearchForm } from '@/components/ui/SearchForm';
import { PaginationControls } from '@/components/ui/PaginationControls';
import Link from 'next/link';
import config from '../config.json';
import { DifficultyGraph } from '@/components/DifficultyGraph';
import { TxIdDisplay } from '@/components/TxIdDisplay';
import { formatHashrate, formatCurrency, formatDate } from '@/lib/utils';

function SystemStatus() {
  const { data: status, isLoading, error } = useQuery({ queryKey: ['status'], queryFn: api.getStatus });

  const { marketCap, supply } = useMemo(() => ({
    marketCap: status ? formatCurrency(status.backend.marketCap || 0) : '$0.00',
    supply: status ? Math.floor(status.backend.supply || 0).toLocaleString('en-US') : '0',
  }), [status]);

  if (isLoading) return <div className="text-center p-8 text-muted-foreground">Loading System Status...</div>;
  if (error) return <div className="text-center p-8 text-destructive">Error loading status</div>;
  if (!status) return null;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-4 gap-4 mb-8">
      <div className="bg-card border border-border p-6 rounded-xl shadow-sm">
        <h3 className="text-muted-foreground text-sm font-medium mb-2 uppercase tracking-wider">Coin Supply</h3>
        <div className="text-2xl font-bold text-card-foreground font-mono">{supply} <span className="text-primary text-base">{config.site.coinSymbol}</span></div>
      </div>
      <div className="bg-card border border-border p-6 rounded-xl shadow-sm">
        <h3 className="text-muted-foreground text-sm font-medium mb-2 uppercase tracking-wider">Network</h3>
        <div className="text-2xl font-bold text-card-foreground font-mono">{formatHashrate(status.backend.hashrate || 0)}</div>
      </div>
      <div className="bg-card border border-border p-6 rounded-xl shadow-sm">
        <h3 className="text-muted-foreground text-sm font-medium mb-2 uppercase tracking-wider">Difficulty</h3>
        <div className="text-2xl font-bold text-card-foreground font-mono">{parseFloat(status.backend.difficulty).toFixed(2)}</div>
      </div>
      <div className="bg-card border border-border p-6 rounded-xl shadow-sm">
        <h3 className="text-muted-foreground text-sm font-medium mb-2 uppercase tracking-wider">Market Cap</h3>
        <div className="text-2xl font-bold text-green-600 dark:text-green-400 font-mono">{marketCap}</div>
      </div>
    </div>
  );
}

function RecentBlocks() {
  const [skip, setSkip] = useState(0);
  const limit = config.ui.pagination?.homepageLimit ?? 10;

  const { data: blocks, isLoading } = useQuery({
    queryKey: ['latestBlocks', skip],
    queryFn: () => api.getLatestBlocks(limit, skip),
    refetchInterval: config.ui.pollingInterval,
    staleTime: config.ui.pollingInterval / 2,
  });

  return (
    <Card title="Latest Blocks" className="bg-card text-card-foreground border-border h-full flex flex-col">
      <div className="flex flex-col flex-1">
        {isLoading && <div className="p-4 text-muted-foreground">Loading blocks...</div>}
        {blocks && blocks.map(block => (
          <li key={block.hash} className="flex flex-row items-center justify-between py-3 border-b border-border last:border-0 hover:bg-muted/50 px-4 transition-colors gap-2">
            <div className="flex w-full items-center justify-between gap-2 lg:hidden">
              <Link href={`/block/${block.height}`} className="font-bold text-primary text-base hover:underline truncate">
                #{block.height}
              </Link>
              <div className="text-sm text-muted-foreground whitespace-nowrap text-center flex-1">
                {block.txCount} txs
              </div>
              <div className="text-sm text-muted-foreground text-right whitespace-nowrap">
                {formatDate(block.time)}
              </div>
            </div>
            <div className="hidden lg:grid grid-cols-[1fr_auto_1fr] items-center w-full gap-4">
              <div className="flex flex-col justify-center">
                <Link href={`/block/${block.height}`} className="font-bold text-primary text-base lg:text-xl hover:underline truncate">
                  #{block.height}
                </Link>
                <div className="text-sm lg:text-base text-muted-foreground whitespace-nowrap">
                  {(block.size / 1024).toFixed(2)} kB
                </div>
              </div>
              <div className="text-sm lg:text-base text-muted-foreground whitespace-nowrap text-center">
                {block.txCount} txs
              </div>
              <div className="text-sm lg:text-base text-muted-foreground text-right whitespace-nowrap">
                {formatDate(block.time)}
              </div>
            </div>
          </li>
        ))}
      </div>
      <PaginationControls
        skip={skip}
        limit={limit}
        itemCount={blocks?.length || 0}
        itemName="Blocks"
        onPrevious={() => setSkip(Math.max(0, skip - limit))}
        onNext={() => setSkip(skip + limit)}
      />
    </Card>
  )
}

function RecentTransactions() {
  const [skip, setSkip] = useState(0);
  const limit = config.ui.pagination?.homepageLimit ?? 10;

  const { data: txs, isLoading } = useQuery({
    queryKey: ['latestTxs', skip],
    queryFn: () => api.getLatestTxs(limit, skip),
    refetchInterval: config.ui.pollingInterval,
    staleTime: config.ui.pollingInterval / 2,
  });

  return (
    <Card title="Latest Transactions" className="bg-card text-card-foreground border-border h-full flex flex-col">
      <div className="flex flex-col flex-1">
        {isLoading && <div className="p-4 text-muted-foreground">Loading transactions...</div>}
        {txs && txs.map(tx => (
          <li key={tx.txid} className="flex flex-col lg:flex-row lg:items-center justify-between py-3 border-b border-border last:border-0 hover:bg-muted/50 px-4 transition-colors gap-2 lg:gap-4">
            <div className="flex flex-col gap-2 w-full lg:hidden">
              <Link
                href={`/tx/${tx.txid}`}
                className="block w-full min-w-0 overflow-hidden font-mono text-muted-foreground hover:text-foreground hover:underline"
                title={tx.txid}
              >
                <TxIdDisplay txid={tx.txid} className="text-sm" />
              </Link>
              <div className="flex items-center justify-between text-sm text-muted-foreground">
                <Link href={`/block/${tx.height}`} className="text-primary hover:underline font-bold">
                  #{tx.height}
                </Link>
                <span className="font-bold text-green-600 dark:text-green-400 whitespace-nowrap">
                  {tx.totalOutput ? tx.totalOutput.toFixed(2) : '0.00'} RXD
                </span>
              </div>
            </div>
            <div className="hidden lg:flex items-center overflow-hidden w-full lg:flex-1">
              <Link href={`/tx/${tx.txid}`} className="block w-full min-w-0 overflow-hidden font-mono text-muted-foreground hover:text-foreground hover:underline" title={tx.txid}>
                <TxIdDisplay txid={tx.txid} className="text-sm lg:text-base" />
              </Link>
            </div>
            <div className="hidden lg:flex flex-col items-start lg:items-end justify-center w-full lg:w-auto min-w-0 lg:min-w-[160px]">
              <span className="font-bold text-green-600 dark:text-green-400 text-base lg:text-xl whitespace-nowrap">
                {tx.totalOutput ? tx.totalOutput.toFixed(2) : '0.00'} RXD
              </span>
              <div className="text-sm lg:text-base text-muted-foreground flex items-center gap-1">
                <Link href={`/block/${tx.height}`} className="text-primary hover:underline font-bold">#{tx.height}</Link>
              </div>
            </div>
          </li>
        ))}
      </div>
      <PaginationControls
        skip={skip}
        limit={limit}
        itemCount={txs?.length || 0}
        itemName="Transactions"
        onPrevious={() => setSkip(Math.max(0, skip - limit))}
        onNext={() => setSkip(skip + limit)}
      />
    </Card>
  )
}

export default function Home() {
  return (
    <div className="flex flex-col gap-8 max-w-7xl mx-auto w-full">
      <h1 className="text-4xl font-bold mb-4 bg-clip-text text-transparent bg-gradient-to-r from-primary to-blue-600">{config.site.title}</h1>
      <SystemStatus />

      <DifficultyGraph />

      <div className="flex justify-center w-full mb-8">
        <SearchForm className="w-full !max-w-full text-lg p-2" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <RecentBlocks />
        <RecentTransactions />
      </div>
    </div>
  );
}
