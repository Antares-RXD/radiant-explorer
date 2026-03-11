'use client'

interface ApiMethodProps {
    title: string;
    description?: string;
    links: { href: string; label: string }[];
    note?: string;
}

function ApiMethod({ title, description, links, note }: ApiMethodProps) {
    return (
        <div className="flex flex-col gap-2 pb-6 border-b border-border last:border-0 last:pb-0">
            <h3 className="text-link font-semibold text-lg">{title}</h3>
            {description && <p className="text-card-foreground">{description}</p>}
            {note && <p className="text-muted-foreground italic text-sm mt-1">{note}</p>}
            <div className="flex flex-col gap-2 mt-2">
                {links.map((link, i) => (
                    <a
                        key={i}
                        href={link.href}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-block bg-muted/50 px-3 py-2 rounded font-mono text-sm text-link hover:opacity-80 hover:underline transition-opacity break-all lg:break-normal"
                    >
                        {link.label}
                    </a>
                ))}
            </div>
        </div>
    );
}

export default function ApiDocs() {
    return (
        <div className="flex flex-col gap-8">
            <h1 className="text-3xl font-bold">API Documentation</h1>

            <section className="bg-card border border-border rounded-lg p-6">
                <h2 className="text-2xl font-semibold text-card-foreground mb-2">API Calls</h2>
                <p className="text-muted-foreground mb-8">Return data from coind</p>

                <div className="flex flex-col gap-8">
                    <ApiMethod
                        title="getdifficulty"
                        description="Returns the current difficulty."
                        links={[{ href: "/api/getdifficulty", label: "/api/getdifficulty" }]}
                    />
                    <ApiMethod
                        title="getconnectioncount"
                        description="Returns the number of connections the block explorer has to other nodes."
                        links={[{ href: "/api/getconnectioncount", label: "/api/getconnectioncount" }]}
                    />
                    <ApiMethod
                        title="getblockcount"
                        description="Returns the current block index."
                        links={[{ href: "/api/getblockcount", label: "/api/getblockcount" }]}
                    />
                    <ApiMethod
                        title="getblockhash [index]"
                        description="Returns the hash of the block at [index]; index 0 is the genesis block."
                        links={[{ href: "/api/getblockhash?index=10000", label: "/api/getblockhash?index=10000" }]}
                    />
                    <ApiMethod
                        title="getblock [hash]"
                        description="Returns information about the block with the given hash."
                        links={[{ href: "/api/getblock?hash=0000000065d8ed5d8be28d6876b3ffb660ac2a6c0ca59e437e1f7a6f4e003fb4", label: "/api/getblock?hash=0000000065d8ed5d8be28d6876b3ffb660ac2a6c0ca59e437e1f7a6f4e003fb4" }]}
                    />
                    <ApiMethod
                        title="getrawtransaction [txid] [decrypt]"
                        description="Returns raw transaction representation for given transaction id. decrypt can be set to 0(false) or 1(true)."
                        links={[
                            { href: "/api/getrawtransaction?txid=027c3f6ae3858a75e2c287a915b3d82a50c48ee7021b715eedae9497f8ba2c37&decrypt=0", label: "/api/getrawtransaction?txid=...&decrypt=0" },
                            { href: "/api/getrawtransaction?txid=027c3f6ae3858a75e2c287a915b3d82a50c48ee7021b715eedae9497f8ba2c37&decrypt=1", label: "/api/getrawtransaction?txid=027c3f6ae3858a75e2c287a915b3d82a50c48ee7021b715eedae9497f8ba2c37&decrypt=1" }
                        ]}
                    />
                    <ApiMethod
                        title="getnetworkhashps"
                        description="Returns the current network hashrate. (hash/s)"
                        links={[{ href: "/api/getnetworkhashps", label: "/api/getnetworkhashps" }]}
                    />
                </div>
            </section>

            <section className="bg-card border border-border rounded-lg p-6">
                <h2 className="text-2xl font-semibold text-card-foreground mb-2">Extended API</h2>
                <p className="text-muted-foreground mb-8">Return data from local indexes</p>

                <div className="flex flex-col gap-8">
                    <ApiMethod
                        title="getmoneysupply"
                        description="Returns current money supply"
                        links={[{ href: "/api/getmoneysupply", label: "/api/getmoneysupply" }]}
                    />
                    <ApiMethod
                        title="getdistribution"
                        description="Returns wealth distribution stats"
                        links={[{ href: "/api/getdistribution", label: "/api/getdistribution" }]}
                    />
                    <ApiMethod
                        title="getaddress (/api/getaddress?address=hash)"
                        description="Returns information for given address"
                        links={[{ href: "/api/getaddress?address=16JR3uTBpTSnhWfLdX8D5EcMrTVhrBCr2X", label: "/api/getaddress?address=16JR3uTBpTSnhWfLdX8D5EcMrTVhrBCr2X" }]}
                    />
                    <ApiMethod
                        title="getaddresstxs (/api/getaddresstxs/hash/start/length)"
                        description="Returns last [length] transactions for address [hash], starting from offset [start]"
                        links={[{ href: "/api/getaddresstxs/16JR3uTBpTSnhWfLdX8D5EcMrTVhrBCr2X/0/50", label: "/api/getaddresstxs/16JR3uTBpTSnhWfLdX8D5EcMrTVhrBCr2X/0/50" }]}
                    />
                    <ApiMethod
                        title="gettx (/api/gettx/hash)"
                        description="Returns information for given tx hash"
                        links={[{ href: "/api/gettx/027c3f6ae3858a75e2c287a915b3d82a50c48ee7021b715eedae9497f8ba2c37", label: "/api/gettx/027c3f6ae3858a75e2c287a915b3d82a50c48ee7021b715eedae9497f8ba2c37" }]}
                    />
                    <ApiMethod
                        title="getbalance (/api/getbalance/hash)"
                        description="Returns current balance of given address"
                        links={[{ href: "/api/getbalance/16JR3uTBpTSnhWfLdX8D5EcMrTVhrBCr2X", label: "/api/getbalance/16JR3uTBpTSnhWfLdX8D5EcMrTVhrBCr2X" }]}
                    />
                    <ApiMethod
                        title="getlasttxs (/api/getlasttxs/min/start/length)"
                        description="Returns last [length] transactions greater than [min] coins, starting from offset [start]"
                        note="Note: [length] is limited to returning 100 records per query"
                        links={[{ href: "/api/getlasttxs/100/0/100", label: "/api/getlasttxs/100/0/100" }]}
                    />
                    <ApiMethod
                        title="getcurrentprice"
                        description="Returns last known exchange price"
                        links={[{ href: "/api/getcurrentprice", label: "/api/getcurrentprice" }]}
                    />
                    <ApiMethod
                        title="getbasicstats"
                        description="Returns basic statistics about the coin including: block count, circulating supply, USD price"
                        links={[{ href: "/api/getbasicstats", label: "/api/getbasicstats" }]}
                    />
                    <ApiMethod
                        title="getsummary"
                        description="Returns a summary of coin data including: difficulty, hybrid difficulty, circulating supply, hash rate, USD price, network connection count, block count"
                        links={[{ href: "/api/getsummary", label: "/api/getsummary" }]}
                    />
                </div>
            </section>

            <section className="bg-card border border-border rounded-lg p-6">
                <h2 className="text-2xl font-semibold text-card-foreground mb-2">Linking (GET)</h2>
                <p className="text-muted-foreground mb-8">Linking to the block explorer</p>

                <div className="flex flex-col gap-8">
                    <ApiMethod
                        title="transaction (/tx/txid)"
                        links={[{ href: "/tx/027c3f6ae3858a75e2c287a915b3d82a50c48ee7021b715eedae9497f8ba2c37", label: "/tx/027c3f6ae3858a75e2c287a915b3d82a50c48ee7021b715eedae9497f8ba2c37" }]}
                    />
                    <ApiMethod
                        title="block (/block/hash)"
                        links={[{ href: "/block/00000001189e35e795f531f86ded4adef83962035e602181355c935734bbf169", label: "/block/00000001189e35e795f531f86ded4adef83962035e602181355c935734bbf169" }]}
                    />
                    <ApiMethod
                        title="address (/address/hash)"
                        links={[{ href: "/address/16JR3uTBpTSnhWfLdX8D5EcMrTVhrBCr2X", label: "/address/16JR3uTBpTSnhWfLdX8D5EcMrTVhrBCr2X" }]}
                    />
                </div>
            </section>
        </div>
    );
}
