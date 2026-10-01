const express = require("express");
const app = express();
app.get("/", (req, res) => res.send("Hello World"));
// 0.0.0.0 = slušaj na svim interfejsima, ne samo unutar kontejnera
app.listen(8080, "0.0.0.0", () => console.log("Slusam na portu 8080"));
