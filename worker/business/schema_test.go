package business_test

import (
	"middleware/worker/business"
	"testing"
)

func TestCSVMappingGames(t *testing.T) {
	ts := `20200,"Galactic Bowling","Oct 21, 2008","0 - 20000",0,0,19.99,0,0,"Galactic Bowling is an exaggerated and stylized bowling game with an intergalactic twist. Players will engage in fast-paced single and multi-player competition while being submerged in a unique new universe filled with over-the-top humor, wild characters, unique levels, and addictive game play. The title is aimed at players of all ages and skill sets. Through accessible and intuitive controls and game-play, Galactic Bowling allows you to jump right into the action. A single-player campaign and online play allow you to work your way up the ranks of the Galactic Bowling League! Whether you have hours to play or only a few minutes, Galactic Bowling is a fast paced and entertaining experience that will leave you wanting more! Full Single-player story campaign including 11 Characters and Environments. 2 Single-player play modes including Regular and Battle Modes. Head to Head Online Multiplayer play Modes. Super Powers, Special Balls, and Whammies. Unlockable Characters, Environments, and Minigames. Unlock all 30 Steam Achievements!","['English']","[]","","https://cdn.akamai.steamstatic.com/steam/apps/20200/header.jpg?t=1640121033","http://www.galacticbowling.net","","",True,False,False,0,"",0,6,11,"",30,0,"",0,0,0,0,"Perpetual FX Creative","Perpetual FX Creative","Single-player,Multi-player,Steam Achievements,Partial Controller Support","Casual,Indie,Sports","Indie,Casual,Sports,Bowling","https://cdn.akamai.steamstatic.com/steam/apps/20200/0000005994.1920x1080.jpg?t=1640121033,https://cdn.akamai.steamstatic.com/steam/apps/20200/0000005993.1920x1080.jpg?t=1640121033,https://cdn.akamai.steamstatic.com/steam/apps/20200/0000005992.1920x1080.jpg?t=1640121033,https://cdn.akamai.steamstatic.com/steam/apps/20200/0000006011.1920x1080.jpg?t=1640121033,https://cdn.akamai.steamstatic.com/steam/apps/20200/0000005685.1920x1080.jpg?t=1640121033,https://cdn.akamai.steamstatic.com/steam/apps/20200/0000005686.1920x1080.jpg?t=1640121033,https://cdn.akamai.steamstatic.com/steam/apps/20200/0000005995.1920x1080.jpg?t=1640121033,https://cdn.akamai.steamstatic.com/steam/apps/20200/0000005688.1920x1080.jpg?t=1640121033,https://cdn.akamai.steamstatic.com/steam/apps/20200/0000005689.1920x1080.jpg?t=1640121033,https://cdn.akamai.steamstatic.com/steam/apps/20200/0000005690.1920x1080.jpg?t=1640121033","http://cdn.akamai.steamstatic.com/steam/apps/256863704/movie_max.mp4?t=1638854607"`
	g, err := business.StrParse[business.Game](ts)

	if err != nil {
		t.Fatalf("Error while reading the csv line")
	}

	if g.AppID != "20200" {
		t.Fatalf(`The game AppId was not properly parsed. %s`, g.AppID)
	}

	if len(g.Categories) != 4 {
		t.Fatalf(`The game Categories was not properly parsed. %s`, g.Categories)
	}
}

func TestCSVMapping(t *testing.T) {
	ts := `10,Counter-Strike,"This will be more of a ''my experience with this game'' type of review, because saying things like ''great gameplay'' will not suit something I've experienced with Counter-Strike. Here you go:  I remember back in 2002 I was at a friend's house and he was playing a game. I didn't know the name of the game nor I had internet to find it. A few weeks passed by and another friend came over. He didn't have a computer, so he brought a disc with a game in it. He told me that it was one of the best games and from that very moment I knew that it is going to be the game I saw at the other friend's house. When I saw the Counter-Strike logo I was filled with gamegasm (?) and I was so happy. I was playing it hardcore. Made friends, clans, was involved in communities and even made two myself. Counter-Strike is my first game which I played competitively and it was a such an experience. Playing public servers with mods were very fun, but playing it competitively made it very intense and stressful. In a pleasant way, ofcourse. Looking at the current e-sport scene it might not seem like much but back then it was different.  Shooters these days try to be different, advanced in a way. Sometimes the most simple games like Counter-Strike are the ones that live to this day. Also, there are plenty of mods to keep your attention to this game. The gameplay is very simple - defend as a Counter-Terrorist, attack as a Terrorist to plant the bomb or save the hostages as a CT. I am sure most of you already know this and I doubt there are gamers that haven't heard or know the gameplay of Counter-Strike, so I am sharing here more of my experience.  I wish I could find my CS Anthology account which I've lost since 2008. So, I decided I am going to buy this game again and here you go - more than a thousand hours played. I still play it from time to time to this day and it brings back many great memories and I sometimes even stumble upon people I've played with years ago. I think Counter-Strike changed gaming in a major way and we wouldn't have many games like we have today, if this game wouldn't exist.   I am sure many of people already have played games like CS:GO but never the roots. I doubt any of you will play it for more than an hour, because it's much more simple and it differs a lot in my opinion from CS:GO and modern games. It's harder though.",1,1`

	g, err := business.StrParse[business.Review](ts)

	if err != nil {
		t.Fatalf("Error while reading the csv line")
	}

	if g.AppID != "10" {
		t.Fatalf(`The game AppId was not properly parsed. %s`, g.AppID)
	}

	if g.AppName != "Counter-Strike" {
		t.Fatalf(`The game Name was not properly parsed. %s`, g.AppName)
	}

	if g.ReviewScore != 1 {
		t.Fatalf(`The gamereview score was not properly parsed. %d`, g.ReviewScore)
	}

	if g.ReviewScore != 1 {
		t.Fatalf(`The gamereview votes was not properly parsed. %d`, g.ReviewVotes)
	}
}
