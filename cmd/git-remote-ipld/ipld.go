package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/magik6k/git-remote-ipld/core"
	ipfs "github.com/ipfs/go-ipfs-api"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"os"
	"strings"
)

const repoKey = ".."

type IpldHandler struct {
	// remoteHash is hash form remote name
	remoteHash string
}

func (h *IpldHandler) List(remote *core.Remote, forPush bool) ([]string, error) {
	headRef, err := remote.Repo.Reference(plumbing.HEAD, false)
	if err != nil {
		return nil, err
	}

	it, err := remote.Repo.Branches()
	if err != nil {
		return nil, err
	}

	out := make([]string, 0)
	var n int
	err = it.ForEach(func(ref *plumbing.Reference) error {
		n++
		trackedRef, err := remote.Tracker.GetRef(ref.Name().String())
		if err != nil {
			return err
		}
		if trackedRef == nil {
			trackedRef = make([]byte, 20)
		}

		// pull ipld::hash, we only want to update HEAD
		if !forPush && headRef.Target() == ref.Name() && headRef.Type() == plumbing.SymbolicReference && len(os.Args) >= 3 {
			sha, err := hex.DecodeString(h.remoteHash)
			if err != nil {
				return err
			}
			if len(sha) != 20 {
				return errors.New("invalid hash length")
			}

			out = append(out, fmt.Sprintf("%s %s", h.remoteHash, headRef.Target().String()))
		} else {
			// For other branches, or if pushing assume value from tracker
			out = append(out, fmt.Sprintf("%s %s", hex.EncodeToString(trackedRef), ref.Name()))
		}

		return nil
	})
	it.Close()
	if err != nil {
		return nil, err
	}

	// For clone
	if n == 0 && !forPush && len(os.Args) >= 3 {
		sha, err := hex.DecodeString(h.remoteHash)
		if err != nil {
			return nil, err
		}
		if len(sha) != 20 {
			return nil, errors.New("invalid hash length")
		}

		out = append(out, fmt.Sprintf("%s %s", h.remoteHash, "refs/heads/master"))
	}

	switch headRef.Type() {
	case plumbing.HashReference:
		out = append(out, fmt.Sprintf("%s %s", headRef.Hash(), headRef.Name()))
	case plumbing.SymbolicReference:
		out = append(out, fmt.Sprintf("@%s %s", headRef.Target().String(), headRef.Name()))
	}

	return out, nil
}

func (h *IpldHandler) Push(remote *core.Remote, local string, remoteRef string) (string, error) {
	localRef, err := remote.Repo.Reference(plumbing.ReferenceName(local), true)
	if err != nil {
		return "", fmt.Errorf("command push: %v", err)
	}

	headHash := localRef.Hash().String()

	push := remote.NewPush()
	err = push.PushHash(headHash)
	if err != nil {
		return "", fmt.Errorf("command push: %v", err)
	}

	hash := localRef.Hash()
	remote.Tracker.SetRef(remoteRef, (&hash)[:])

	c, err := core.CidFromHex(headHash)
	if err != nil {
		return "", fmt.Errorf("push: %v", err)
	}

	repoCid, err := addRefToRepoObject(remote, remoteRef, c.String())
	if err != nil {
		return "", fmt.Errorf("push: %v", err)
	}

	remote.Logger.Printf("Pushed to IPFS as \x1b[32mipld::%s\x1b[39m\n", headHash)
	remote.Logger.Printf("Head CID is %s\n", c.String())
	remote.Logger.Printf("Repo CID is %s\n", repoCid)
	return local, nil
}

func insertRefIntoMap(m map[string]interface{}, ref string, cid string) map[string]interface{} {
	split := strings.Split(ref, "/")
	currMap := m
	for _, seg := range split {
		if _, ok := currMap[seg]; !ok {
			currMap[seg] = make(map[string]interface{})
		}
		currMap = currMap[seg].(map[string]interface{})
	}
	currMap["/"] = cid
	return m
}

func addRefToRepoObject(remote *core.Remote, refName, cid string) (string, error) {
	api := ipfs.NewLocalShell()

	// store the repo object under the ".." key, works since ".." isnt allowed
	// in git ref names
	repoCid, err := remote.Tracker.GetRef(repoKey)
	if err != nil {
		return "", fmt.Errorf("push: %v", err)
	}

	repoMap := make(map[string]interface{})
	if repoCid != nil {
		err = api.DagGet(string(repoCid), &repoMap)
		if err != nil {
			// TODO: if error is nil, is repoMap guaranteed to be unchanged?
			repoMap = make(map[string]interface{})
		}
	}

	repoMap = insertRefIntoMap(repoMap, refName, cid)

	ipldJson, err := json.Marshal(repoMap)
	if err != nil {
		return "", fmt.Errorf("push: %v", err)
	}

	res, err := api.DagPut(ipldJson, "json", "cbor")
	if err != nil {
		return "", fmt.Errorf("push: %v", err)
	}

	return res, remote.Tracker.SetRef(repoKey, []byte(res))
}
