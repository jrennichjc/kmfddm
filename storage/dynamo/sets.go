package dynamo


import (
	"context"
	"fmt"
	"strings"
)

// RetrieveSetDeclarations returns a slice of declaration IDs that are associated with setName.
func (s *DSDynamoTable) RetrieveSetDeclarations(_ context.Context, setName string) ([]string, error) {
	
	var declarations []string
	var items []dsPKSKOnly

	err := s.GetItemsSKBeginsWith("set#" + setName, "dec#", &items)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		declarations = append(declarations, strings.TrimPrefix(item.SK, "dec#"))
	}
	return declarations, err
	
}

// StoreSetDeclaration creates the association between a declaration and a set.
func (s *DSDynamoTable) StoreSetDeclaration(_ context.Context, setName, declarationID string) (bool, error) {
	
	var decItem dsDeclaration

	err := s.GetSingleItemPKSK("dec#" + declarationID, "dec#", &decItem)

	if err != nil || decItem.JSON == "" {
		return false, fmt.Errorf("checking declaration: %w", err)
	}

	declarationSetItem := dsGenericItem {
		PK: "set#" + setName,
		SK: "dec#" + declarationID,
	}

	err = s.AddItem(declarationSetItem)

	//TODO: determine if the set has actually changed
	return true, err
}

// RemoveSetDeclaration removes the association between a declaration and a set.
func (s *DSDynamoTable) RemoveSetDeclaration(_ context.Context, setName, declarationID string) (bool, error) {

	item := dsGenericItem {
		PK: "set#" + setName,
		SK: "dec#" + declarationID,
	}

	s.deleteSingleItem(item)

	//TODO: determine if the set has actually changed
	return true, nil
}

// RetrieveSets retrieves the list of all sets.
func (s *DSDynamoTable) RetrieveSets(_ context.Context) ([]string, error) {

	var items []dsGenericItem
	var sets []string

	err := s.GetItemsSKBeginsWith("set", "set#", &items)

	if err != nil {
		return nil, err
	}

	for _, item := range items {
		sets = append(sets, strings.TrimPrefix(item.SK, "set#"))
	}

	return sets, err
}

// RetrieveDeclarationSets returns the list of sets associated with a declaration.
func (s *DSDynamoTable) RetrieveDeclarationSets(_ context.Context, declarationID string) ([]string, error) {
	var items []dsGenericItem
	var sets []string

	err := s.GetReverseItems("dec#" + declarationID, "set#", &items)

	if err != nil {
		return sets, err
	}
	
	for _, item := range items {
		sets = append(sets, strings.TrimPrefix(item.PK, "set#"))
	}

	return sets, err
}
