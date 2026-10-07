//go:build windows
// +build windows

package iso9660

//nolint:revive // prefer named unused parameter
func statt(sys interface{}) (links, uid, gid uint32) {
	return uint32(0), uint32(0), uint32(0)
}
