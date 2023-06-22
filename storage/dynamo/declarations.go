package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"

	"github.com/jessepeterson/kmfddm/ddm"
)

type dsDeclaration struct {
	PK    string `dynamodbav:"pk"`
	SK    string `dynamodbav:"sk"`
	JSON  string `dynamodbav:"json"`
	Token string `dynamodbav:"dec_token"`
	Type  string `dynamodbav:"dec_type"`
	Salt  string `dynamodbav:"salt"`
}

func (s *DSDynamoTable) StoreDeclaration(_ context.Context, d *ddm.Declaration) (bool, error) {

	var token string
	var creationSalt []byte
	var tokenMissing bool

	var item dsDeclaration

	err := s.GetItemsSKBeginsWith("dec#"+d.Identifier, "dec#", &item)
	if item.Token != "" && item.Salt != "" {
		token = item.Token
		creationSalt = []byte(item.Salt)
	} else {
		tokenMissing = true
		// token is missing, lets make a new salt
		creationSalt = make([]byte, 32)
		if _, err = rand.Read(creationSalt); err != nil {
			return false, fmt.Errorf("reading random data for creation salt: %w", err)
		}
	}

	if err != nil {
		return false, fmt.Errorf("reading declaration: %w", err)
	}

	// unmarshal the uploaded declaration
	var declaration map[string]interface{}
	if err = json.Unmarshal(d.Raw, &declaration); err != nil {
		return false, err
	}

	// remove the servertoken to make the marshaling idempotent
	delete(declaration, "ServerToken")

	// re-marshal (without a servertoken)
	dBytes, err := json.Marshal(&declaration)
	if err != nil {
		return false, fmt.Errorf("marshaling no-token declaration: %w", err)
	}

	// hash the marshaled declaration (without token and with creation salt)
	hasher := s.newHash()
	_, err = hasher.Write(append(dBytes, creationSalt...))
	if err != nil {
		return false, fmt.Errorf("hashing marshaled no-token declaration: %w", err)
	}
	dHash := fmt.Sprintf("%x", hasher.Sum(nil))

	if !tokenMissing && dHash == token {
		// the hashed version of our profile is the same
		// as the token we already have. bail telling the caller there
		// were no changes.
		return false, nil
	}
	token = dHash

	declaration["ServerToken"] = token

	// marshal the declaration (with the new token)
	dBytes, err = json.Marshal(&declaration)
	if err != nil {
		return false, fmt.Errorf("marshaling declaration: %w", err)
	}

	newDeclaration := dsDeclaration{
		PK:    "dec#" + d.Identifier,
		SK:    "dec#",
		JSON:  string(dBytes),
		Token: token,
		Salt:  item.Salt,
		Type:  d.Type,
	}

	if tokenMissing {
		newDeclaration.Salt = string(creationSalt)
	}

	err = s.AddItem(newDeclaration)

	if err != nil {
		return false, err
	}

	if err = s.writeDeclarationDDM(d.Identifier); err != nil {
		return false, err
	}

	return true, nil
}

// RetrieveDeclaration retrieves a declaration by its ID.
func (s *DSDynamoTable) RetrieveDeclaration(_ context.Context, declarationID string) (*ddm.Declaration, error) {

	var declaration dsDeclaration

	err := s.GetSingleItemPKSK("dec#"+declarationID, "dec#", &declaration)

	if err != nil {
		return nil, err
	}

	d, err := ddm.ParseDeclaration([]byte(declaration.JSON))
	if err != nil {
		return nil, fmt.Errorf("parsing declaration: %w", err)
	}
	return d, nil
}

// DeleteDeclaration deletes a declaration by its ID.
func (s *DSDynamoTable) DeleteDeclaration(_ context.Context, identifier string) (bool, error) {

	var declaration dsDeclaration

	err := s.GetSingleItemPKSK("dec#"+identifier, "dec#", &declaration)

	if err != nil {
		return false, err
	}

	s.deleteSingleItem(declaration)

	var items []dsGenericItem

	err = s.GetReverseItems("dec#"+identifier, "set#", &items)

	if err != nil {
		return false, err
	}

	for _, item := range items {
		s.deleteSingleItem(item)
	}

	return true, nil
}

// RetrieveDeclarations retrieves a slice of all declaration IDs.
func (s *DSDynamoTable) RetrieveDeclarations(_ context.Context) ([]string, error) {

	var declarations []dsDeclaration
	var results []string

	err := s.GetReverseItems("dec#", "dec#", &declarations)

	if err != nil {
		return nil, err
	}

	for _, dec := range declarations {
		results = append(results, strings.TrimPrefix(dec.PK, "dec#"))
	}

	return results, nil
}
