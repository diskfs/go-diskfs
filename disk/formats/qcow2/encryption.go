package qcow2

import "fmt"

type encryptionMethod uint32

const (
	encryptionMethodNone encryptionMethod = 0
	encryptionMethodAES                   = 1
	encryptionMethodLUKS                  = 2
)

type Encryptor interface {
	method() encryptionMethod
	hasHeader() bool
}

type EncryptorAES struct{}

func (e EncryptorAES) method() encryptionMethod {
	return encryptionMethodAES
}
func (e EncryptorAES) hasHeader() bool {
	return false
}

type EncryptorLUKS struct{}

func (e EncryptorLUKS) method() encryptionMethod {
	return encryptionMethodLUKS
}
func (e EncryptorLUKS) hasHeader() bool {
	return true
}

func newEncryptor(method encryptionMethod) (Encryptor, error) {
	var e Encryptor
	switch method {
	case encryptionMethodNone:
		e = nil
	case encryptionMethodAES:
		e = &EncryptorAES{}
	case encryptionMethodLUKS:
		e = &EncryptorLUKS{}
	default:
		return nil, fmt.Errorf("Unknown encryption method: %d", method)
	}
	return e, nil
}
