package core

import (
	"container/list"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	ipfs "github.com/ipfs/go-ipfs-api"
	ipldgit "github.com/ipfs/go-ipld-git"
	git "gopkg.in/src-d/go-git.v4"

	"gopkg.in/src-d/go-git.v4/plumbing"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
)

type Push struct {
	objectDir string
	gitDir    string

	done    uint64
	todoc   uint64
	todo    *list.List
	log     *log.Logger
	tracker *Tracker
	repo    *git.Repository
}

func NewPush(gitDir string, tracker *Tracker, repo *git.Repository) *Push {
	return &Push{
		objectDir: path.Join(gitDir, "objects"),
		gitDir:    gitDir,
		todo:      list.New(),
		log:       log.New(os.Stderr, "push: ", 0),
		tracker:   tracker,
		repo:      repo,
		todoc:     1,
	}
}

func (p *Push) PushHash(hash string) error {
	p.todo.PushFront(hash)
	return p.doWork()
}

func (p *Push) doWork() error {
	api := ipfs.NewLocalShell()

	for e := p.todo.Front(); e != nil; e = e.Next() {
		hash := e.Value.(string)

		sha, err := hex.DecodeString(hash)
		if err != nil {
			return fmt.Errorf("push: %v", err)
		}

		has, err := p.tracker.HasEntry(sha)
		if err != nil {
			return fmt.Errorf("push/process: %v", err)
		}

		if has {
			p.todoc--
			continue
		}

		expectedCid, err := CidFromHex(hash)
		if err != nil {
			return fmt.Errorf("push: %v", err)
		}

		obj, err := p.repo.Storer.EncodedObject(plumbing.AnyObject, plumbing.NewHash(hash))
		if err != nil {
			return fmt.Errorf("push: %v", err)
		}

		rawReader, err := obj.Reader()
		if err != nil {
			return fmt.Errorf("push: %v", err)
		}

		raw, err := ioutil.ReadAll(rawReader)
		if err != nil {
			return fmt.Errorf("push: %v", err)
		}

		switch obj.Type() {
		case plumbing.CommitObject:
			raw = append([]byte(fmt.Sprintf("commit %d\x00", obj.Size())), raw...)
		case plumbing.TreeObject:
			raw = append([]byte(fmt.Sprintf("tree %d\x00", obj.Size())), raw...)
		case plumbing.BlobObject:
			raw = append([]byte(fmt.Sprintf("blob %d\x00", obj.Size())), raw...)
		case plumbing.TagObject:
			raw = append([]byte(fmt.Sprintf("tag %d\x00", obj.Size())), raw...)
		}

		p.done++
		p.log.Printf("%d/%d %s %s\r\x1b[A", p.done, p.todoc, hash, expectedCid.String())

		res, err := api.DagPut(raw, "raw", "git")
		if err != nil {
			return fmt.Errorf("push: %v", err)
		}

		err = p.tracker.AddEntry(sha)
		if err != nil {
			return fmt.Errorf("push: %v", err)
		}

		if expectedCid.String() != res {
			return fmt.Errorf("CIDs don't match: expected %s, got %s", expectedCid.String(), res)
		}

		p.processLinks(raw)
	}
	p.log.Printf("\n")
	return nil
}

func (p *Push) processLinks(object []byte) error {
	nd, err := ipldgit.ParseObjectFromBuffer(object)
	if err != nil {
		return fmt.Errorf("push/process: %v", err)
	}

	links := nd.Links()
	for _, link := range links {
		mhash := link.Cid.Hash()
		decoded, err := mh.Decode(mhash)
		if err != nil {
			return fmt.Errorf("push/process: %v", err)
		}

		has, err := p.tracker.HasEntry(decoded.Digest)
		if err != nil {
			return fmt.Errorf("push/process: %v", err)
		}

		if has {
			continue
		}

		p.todoc++
		p.todo.PushBack(hex.EncodeToString(decoded.Digest))
	}
	return nil
}
