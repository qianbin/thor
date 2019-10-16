// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/inconshreveable/log15"
	isatty "github.com/mattn/go-isatty"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/vechain/thor/api"
	"github.com/vechain/thor/block"
	"github.com/vechain/thor/chain"
	"github.com/vechain/thor/cmd/thor/node"
	"github.com/vechain/thor/cmd/thor/solo"
	"github.com/vechain/thor/genesis"
	"github.com/vechain/thor/kv"
	"github.com/vechain/thor/logdb"
	"github.com/vechain/thor/lvldb"
	"github.com/vechain/thor/state"
	"github.com/vechain/thor/thor"
	"github.com/vechain/thor/trie"
	"github.com/vechain/thor/triex"
	"github.com/vechain/thor/txpool"
	"gopkg.in/cheggaaa/pb.v1"
	cli "gopkg.in/urfave/cli.v1"
)

var (
	version   string
	gitCommit string
	gitTag    string
	log       = log15.New()

	defaultTxPoolOptions = txpool.Options{
		Limit:           10000,
		LimitPerAccount: 16,
		MaxLifetime:     20 * time.Minute,
	}
)

func fullVersion() string {
	versionMeta := "release"
	if gitTag == "" {
		versionMeta = "dev"
	}
	return fmt.Sprintf("%s-%s-%s", version, gitCommit, versionMeta)
}

func main() {
	app := cli.App{
		Version:   fullVersion(),
		Name:      "Thor",
		Usage:     "Node of VeChain Thor Network",
		Copyright: "2018 VeChain Foundation <https://vechain.org/>",
		Flags: []cli.Flag{
			networkFlag,
			configDirFlag,
			dataDirFlag,
			cacheFlag,
			beneficiaryFlag,
			targetGasLimitFlag,
			apiAddrFlag,
			apiCorsFlag,
			apiTimeoutFlag,
			apiCallGasLimitFlag,
			apiBacktraceLimitFlag,
			verbosityFlag,
			maxPeersFlag,
			p2pPortFlag,
			natFlag,
			bootNodeFlag,
			skipLogsFlag,
			pprofFlag,
			verifyLogsFlag,
		},
		Action: defaultAction,
		Commands: []cli.Command{
			{
				Name:  "solo",
				Usage: "client runs in solo mode for test & dev",
				Flags: []cli.Flag{
					dataDirFlag,
					apiAddrFlag,
					apiCorsFlag,
					apiTimeoutFlag,
					apiCallGasLimitFlag,
					apiBacktraceLimitFlag,
					onDemandFlag,
					persistFlag,
					gasLimitFlag,
					verbosityFlag,
					pprofFlag,
					verifyLogsFlag,
				},
				Action: soloAction,
			},
			{
				Name:  "master-key",
				Usage: "master key management",
				Flags: []cli.Flag{
					configDirFlag,
					importMasterKeyFlag,
					exportMasterKeyFlag,
				},
				Action: masterKeyAction,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func prune(triex *triex.Proxy, rawDB kv.GetPutter, chain *chain.Chain) error {

	gen := uint32(0)
	go func() {
		bb := thor.NewBigBloom(256, 3)
		for {
			for {
				if chain.BestBlock().Header().Number()/100000 >= gen+2 {
					break
				}
				time.Sleep(time.Second)
			}
			n1 := gen * 100000
			n2 := (gen + 1) * 100000
			gen++

			fmt.Printf("Pruner: start [%v, %v]\n", n1, n2)

			indexTrieEntries := 0
			accountTrieEntries := 0
			storageTrieEntries := 0

			var prefix []byte
			// index trie

			id1, err := chain.NewTrunk().GetBlockID(n1)
			if err != nil {
				panic(err)
			}
			id2, err := chain.NewTrunk().GetBlockID(n2)
			if err != nil {
				panic(err)
			}

			h1, root1, err := chain.GetBlockHeader(id1)
			if err != nil {
				panic(err)
			}
			h2, root2, err := chain.GetBlockHeader(id2)
			if err != nil {
				panic(err)
			}
			{
				it1, err := triex.NewTrie(root1, n1, false).NodeIterator(nil)
				if err != nil {
					panic(err)
				}
				it2, err := triex.NewTrie(root2, n2, false).NodeIterator(nil)
				if err != nil {
					panic(err)
				}
				it, _ := trie.NewDifferenceIterator(it1, it2)
				for it.Next(true) {
					if h := it.Hash(); !h.IsZero() {
						indexTrieEntries++
						bb.Add(h)
					}
				}
				if err := it.Error(); err != nil {
					panic(err)
				}
				fmt.Println("Pruner: index trie entries", indexTrieEntries)
			}

			// accounts
			{
				tr1 := triex.NewTrie(h1.StateRoot(), n1, false)
				it1, err := tr1.NodeIterator(nil)
				if err != nil {
					panic(err)
				}
				tr2 := triex.NewTrie(h2.StateRoot(), n2, false)
				it2, err := tr2.NodeIterator(nil)
				if err != nil {
					panic(err)
				}

				prefix = tr1.Prefix()

				it, _ := trie.NewDifferenceIterator(it1, it2)
				for it.Next(true) {
					if h := it.Hash(); !h.IsZero() {
						bb.Add(h)
						accountTrieEntries++
					}
					if it.Leaf() {
						blob2 := it.LeafBlob()
						var acc2 state.Account
						if err := rlp.DecodeBytes(blob2, &acc2); err != nil {
							panic(err)
						}

						if len(acc2.StorageRoot) > 0 {
							sroot2 := thor.BytesToBytes32(acc2.StorageRoot)
							blob1, err := tr1.Get(it.LeafKey())
							if err != nil {
								panic(err)
							}
							var sroot1 thor.Bytes32
							if len(blob1) > 0 {
								var acc1 state.Account
								if err := rlp.DecodeBytes(blob1, &acc1); err != nil {
									panic(err)
								}
								sroot1 = thor.BytesToBytes32(acc1.StorageRoot)
							}
							sit1, err := triex.NewTrie(sroot1, n1, false).NodeIterator(nil)
							if err != nil {
								panic(err)
							}
							sit2, err := triex.NewTrie(sroot2, n2, false).NodeIterator(nil)
							if err != nil {
								panic(err)
							}
							sit, _ := trie.NewDifferenceIterator(sit1, sit2)
							for sit.Next(true) {
								if h := sit.Hash(); !h.IsZero() {
									storageTrieEntries++
									bb.Add(h)
								}
							}
							if err := sit.Error(); err != nil {
								panic(err)
							}
						}
					}
				}
				if err := it.Error(); err != nil {
					panic(err)
				}
				fmt.Println("Pruner: account trie entries", accountTrieEntries)
				fmt.Println("Pruner: storage trie entries", storageTrieEntries)
			}

			fmt.Printf("Pruner: deleting prefix %x...\n", prefix)
			scaned := 0
			deleted := 0
			rng := *kv.NewRangeWithBytesPrefix(prefix)
			prefixLen := len(prefix)

			rawIt := rawDB.NewIterator(rng)
			for rawIt.Next() {
				scaned++
				k := rawIt.Key()
				if !bb.Test(thor.BytesToBytes32(k[prefixLen:])) {
					rawDB.Delete(k)
					deleted++
				}
			}
			rawIt.Release()

			fmt.Println("Pruner: deleted", deleted, "/", scaned, "entries")
			fmt.Println("Pruner: do compact")
			if err := rawDB.(*lvldb.LevelDB).CompactRange(rng); err != nil {
				fmt.Println(err)
			}
			fmt.Println("Pruner: compact done")
		}
	}()

	return nil
}

func defaultAction(ctx *cli.Context) error {
	exitSignal := handleExitSignal()

	defer func() { log.Info("exited") }()

	initLogger(ctx)
	gene, forkConfig := selectGenesis(ctx)
	instanceDir := makeInstanceDir(ctx, gene)

	chainDB := openChainDB(ctx, instanceDir)
	defer func() { log.Info("closing chain database..."); chainDB.Close() }()

	stateDB, trieCacheSizeMB := openStateDB(ctx, instanceDir)
	defer func() { log.Info("closing state database..."); stateDB.Close() }()

	// f := func(p byte) {
	// 	n := 0
	// 	size := 0
	// 	it := stateDB.NewIterator(*kv.NewRangeWithBytesPrefix([]byte{p}))
	// 	for it.Next() {
	// 		n++
	// 		size += len(it.Value())
	// 	}
	// 	it.Release()
	// 	fmt.Println(n, size)
	// }
	// f(0)
	// f(1)
	// f(2)
	// f(3)

	skipLogs := ctx.Bool(skipLogsFlag.Name)

	logDB := openLogDB(ctx, instanceDir)
	defer func() { log.Info("closing log database..."); logDB.Close() }()

	triex := triex.New(stateDB, trieCacheSizeMB)

	chain := initChain(gene, chainDB, triex, logDB)

	// _, indexRoot, _ := chain.GetBlockHeader(chain.BestBlock().Header().ID())

	// it, _ := triex.NewTrie(indexRoot, false).NodeIterator(nil)
	// n := 0

	// for it.Next(true) {
	// 	if h := it.Hash(); !h.IsZero() {
	// 		// fmt.Println(h)
	// 		n++
	// 	}
	// }

	// fmt.Println(n)
	// return nil
	prune(triex, stateDB, chain)

	master := loadNodeMaster(ctx)

	printStartupMessage1(gene, chain, master, instanceDir, forkConfig)

	if !skipLogs {
		if err := syncLogDB(exitSignal, chain, logDB, ctx.Bool(verifyLogsFlag.Name)); err != nil {
			return err
		}
	}

	txPool := txpool.New(chain, triex, defaultTxPoolOptions)
	defer func() { log.Info("closing tx pool..."); txPool.Close() }()

	p2pcom := newP2PComm(ctx, chain, txPool, instanceDir)
	apiHandler, apiCloser := api.New(
		chain,
		triex,
		txPool,
		logDB,
		p2pcom.comm,
		ctx.String(apiCorsFlag.Name),
		uint32(ctx.Int(apiBacktraceLimitFlag.Name)),
		uint64(ctx.Int(apiCallGasLimitFlag.Name)),
		ctx.Bool(pprofFlag.Name),
		skipLogs,
		forkConfig)
	defer func() { log.Info("closing API..."); apiCloser() }()

	apiURL, srvCloser := startAPIServer(ctx, apiHandler, chain.GenesisBlock().Header().ID())
	defer func() { log.Info("stopping API server..."); srvCloser() }()

	printStartupMessage2(apiURL, getNodeID(ctx))

	p2pcom.Start()
	defer p2pcom.Stop()

	return node.New(
		master,
		chain,
		triex,
		logDB,
		txPool,
		filepath.Join(instanceDir, "tx.stash"),
		p2pcom.comm,
		uint64(ctx.Int(targetGasLimitFlag.Name)),
		skipLogs,
		forkConfig).
		Run(exitSignal)
}

func soloAction(ctx *cli.Context) error {
	exitSignal := handleExitSignal()
	defer func() { log.Info("exited") }()

	initLogger(ctx)
	gene := genesis.NewDevnet()
	// Solo forks from the start
	forkConfig := thor.ForkConfig{}

	var (
		chainDB         kv.GetPutCloser
		stateDB         kv.GetPutCloser
		trieCacheSizeMB int
		logDB           *logdb.LogDB
		instanceDir     string
	)

	if ctx.Bool("persist") {
		instanceDir = makeInstanceDir(ctx, gene)
		chainDB = openChainDB(ctx, instanceDir)
		stateDB, trieCacheSizeMB = openStateDB(ctx, instanceDir)
		logDB = openLogDB(ctx, instanceDir)
	} else {
		instanceDir = "Memory"
		chainDB = openMemDB()
		stateDB = openMemDB()
		logDB = openMemLogDB()
	}

	defer func() { log.Info("closing chain database..."); chainDB.Close() }()
	defer func() { log.Info("closing state database..."); stateDB.Close() }()
	defer func() { log.Info("closing log database..."); logDB.Close() }()

	triex := triex.New(stateDB, trieCacheSizeMB)

	chain := initChain(gene, chainDB, triex, logDB)
	if err := syncLogDB(exitSignal, chain, logDB, ctx.Bool(verifyLogsFlag.Name)); err != nil {
		return err
	}

	txPool := txpool.New(chain, triex, defaultTxPoolOptions)
	defer func() { log.Info("closing tx pool..."); txPool.Close() }()

	apiHandler, apiCloser := api.New(
		chain,
		triex,
		txPool,
		logDB,
		solo.Communicator{},
		ctx.String(apiCorsFlag.Name),
		uint32(ctx.Int(apiBacktraceLimitFlag.Name)),
		uint64(ctx.Int(apiCallGasLimitFlag.Name)),
		ctx.Bool(pprofFlag.Name),
		false,
		forkConfig)
	defer func() { log.Info("closing API..."); apiCloser() }()

	apiURL, srvCloser := startAPIServer(ctx, apiHandler, chain.GenesisBlock().Header().ID())
	defer func() { log.Info("stopping API server..."); srvCloser() }()

	printSoloStartupMessage(gene, chain, instanceDir, apiURL, forkConfig)

	return solo.New(chain,
		triex,
		logDB,
		txPool,
		uint64(ctx.Int("gas-limit")),
		ctx.Bool("on-demand"),
		forkConfig).Run(exitSignal)
}

func masterKeyAction(ctx *cli.Context) error {
	hasImportFlag := ctx.Bool(importMasterKeyFlag.Name)
	hasExportFlag := ctx.Bool(exportMasterKeyFlag.Name)
	if hasImportFlag && hasExportFlag {
		return fmt.Errorf("flag %s and %s are exclusive", importMasterKeyFlag.Name, exportMasterKeyFlag.Name)
	}

	if !hasImportFlag && !hasExportFlag {
		masterKey, err := loadOrGeneratePrivateKey(masterKeyPath(ctx))
		if err != nil {
			return err
		}
		fmt.Println("Master:", thor.Address(crypto.PubkeyToAddress(masterKey.PublicKey)))
		return nil
	}

	if hasImportFlag {
		if isatty.IsTerminal(os.Stdin.Fd()) {
			fmt.Println("Input JSON keystore (end with ^d):")
		}
		keyjson, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(keyjson, &map[string]interface{}{}); err != nil {
			return errors.WithMessage(err, "unmarshal")
		}
		password, err := readPasswordFromNewTTY("Enter passphrase: ")
		if err != nil {
			return err
		}

		key, err := keystore.DecryptKey(keyjson, password)
		if err != nil {
			return errors.WithMessage(err, "decrypt")
		}

		if err := crypto.SaveECDSA(masterKeyPath(ctx), key.PrivateKey); err != nil {
			return err
		}
		fmt.Println("Master key imported:", thor.Address(key.Address))
		return nil
	}

	if hasExportFlag {
		masterKey, err := loadOrGeneratePrivateKey(masterKeyPath(ctx))
		if err != nil {
			return err
		}

		password, err := readPasswordFromNewTTY("Enter passphrase: ")
		if err != nil {
			return err
		}
		if password == "" {
			return errors.New("non-empty passphrase required")
		}
		confirm, err := readPasswordFromNewTTY("Confirm passphrase: ")
		if err != nil {
			return err
		}

		if password != confirm {
			return errors.New("passphrase confirmation mismatch")
		}

		keyjson, err := keystore.EncryptKey(&keystore.Key{
			PrivateKey: masterKey,
			Address:    crypto.PubkeyToAddress(masterKey.PublicKey),
			Id:         uuid.NewRandom()},
			password, keystore.StandardScryptN, keystore.StandardScryptP)
		if err != nil {
			return err
		}
		if isatty.IsTerminal(os.Stdout.Fd()) {
			fmt.Println("=== JSON keystore ===")
		}
		_, err = fmt.Println(string(keyjson))
		return err
	}
	return nil
}

func seekLogDBSyncPosition(chain *chain.Chain, logDB *logdb.LogDB) (uint32, error) {
	best := chain.BestBlock().Header()
	if best.Number() == 0 {
		return 0, nil
	}

	newestID, err := logDB.NewestBlockID()
	if err != nil {
		return 0, err
	}

	if block.Number(newestID) == 0 {
		return 0, nil
	}

	if newestID == best.ID() {
		return best.Number(), nil
	}

	seekStart := block.Number(newestID)
	if seekStart >= best.Number() {
		seekStart = best.Number() - 1
	}

	header, err := chain.NewTrunk().GetBlockHeader(seekStart)
	if err != nil {
		return 0, err
	}

	for header.Number() > 0 {
		has, err := logDB.HasBlockID(header.ID())
		if err != nil {
			return 0, err
		}
		if has {
			break
		}

		header, _, err = chain.GetBlockHeader(header.ParentID())
		if err != nil {
			return 0, err
		}
	}
	return block.Number(header.ID()) + 1, nil

}

func syncLogDB(ctx context.Context, chain *chain.Chain, logDB *logdb.LogDB, verify bool) error {
	startPos, err := seekLogDBSyncPosition(chain, logDB)
	if err != nil {
		return errors.Wrap(err, "seek log db sync position")
	}
	if verify && startPos > 0 {
		if err := verifyLogDB(ctx, startPos-1, chain, logDB); err != nil {
			return errors.Wrap(err, "verify log db")
		}
	}

	bestNum := chain.BestBlock().Header().Number()

	if bestNum == startPos {
		return nil
	}

	if startPos == 0 {
		fmt.Println(">> Rebuilding log db <<")
		startPos = 1 // block 0 can be skipped
	} else {
		fmt.Println(">> Syncing log db <<")
	}

	pb := pb.New64(int64(bestNum)).
		Set64(int64(startPos - 1)).
		SetMaxWidth(90).
		Start()

	defer func() { pb.NotPrint = true }()

	it := chain.NewIterator(256).Seek(startPos)

	task := logDB.NewTask()
	taskLen := 0

	for it.Next() {
		b := it.Block()

		task.ForBlock(b.Header())
		txs := b.Transactions()
		if len(txs) > 0 {
			receipts, err := chain.GetReceipts(b.Header().ID())
			if err != nil {
				return errors.Wrap(err, "get block receipts")
			}

			for i, tx := range txs {
				origin, _ := tx.Origin()
				task.Write(tx.ID(), origin, receipts[i].Outputs)
				taskLen++
			}
		}
		if taskLen > 512 {
			if err := task.Commit(); err != nil {
				return errors.Wrap(err, "write logs")
			}
			task = logDB.NewTask()
			taskLen = 0
		}
		pb.Add64(1)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	if taskLen > 0 {
		if err := task.Commit(); err != nil {
			return errors.Wrap(err, "write logs")
		}
	}

	if err := it.Error(); err != nil {
		return errors.Wrap(err, "read block")
	}
	pb.Finish()
	return nil
}
