package worker

import (
	"fmt"
	"middleware/worker/business"
)

func main() {
	var g business.Game

	fmt.Print(g.AppID)
}
