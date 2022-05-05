package cavalier

import "errors"

var errDBNotCreatedByCavalier = errors.New("cavalier: the specified DB instance wasn't created by the cavalier")
