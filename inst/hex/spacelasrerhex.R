library(ggplot2)
library(cropcircles)
library(rnaturalearth)
library(sf)
library(purrr)
library(tibble)
library(png)
library(ggsvg)

world <- ne_countries(scale = "medium", returnclass = "sf") |>
  wk::wk_flatten()

world_orth <- sf::st_transform(
  world,
  sf::st_crs(
    "+proj=ortho +lat_0=10 +lon_0=22 +x_0=0 +y_0=0 +"
  )
)

belian_png <- png::readPNG("inst/hex/belian.png")
belian_alpha <- 0.05
belian_png[,, 4] <- belian_png[,, 4] * belian_alpha

# Satellite SVG from svgrepo.com, recoloured white. The source has
# fill="#000000" at the <svg> element; replace it so the mark reads
# against the dark plot background.
sat_svg <- paste(
  readLines("inst/hex/satellite-svgrepo-com.svg"),
  collapse = "\n"
)
sat_svg <- gsub('fill="#000000"', 'fill="#ffffff"', sat_svg)

txtyc <- 80000

# Belian mark extent. The PNG is ~square (208 x 210), so a square
# xmin/xmax/ymin/ymax keeps its aspect ratio under coord_sf. Tune
# belian_radius to resize; shift the centre via belian_cx / belian_cy.
belian_radius <- 71e5
belian_cx <- 0
belian_cy <- txtyc + 1e5


p <- ggplot() +
  annotation_raster(
    belian_png,
    xmin = belian_cx - belian_radius,
    xmax = belian_cx + belian_radius,
    ymin = belian_cy - belian_radius,
    ymax = belian_cy + belian_radius
  ) +
  geom_sf(
    data = world_orth,
    fill = "#74ac90ff",
    colour = "#dadada0a",
    alpha = 0.3
  ) +

  # Satellite: positioned above-right, drawn over the earth so the
  # white mark reads cleanly against the darker background.
  geom_point_svg(
    aes(x = 7.5e6, y = 4.9e6),
    svg = sat_svg,
    size = 18
  ) +

  geom_polygon(
    data = data.frame(
      x = c(4.75e6, 4.55e6, 6.4e6),
      y = c(2.044e6, 2.35e6, 3.8e6)
    ),
    aes(x = x, y = y),
    fill = "#DB830B",
    colour = "white",
    alpha = 1,
    lwd = 0.1
  ) +
  geom_text(
    aes(
      x = 0,
      y = txtyc,
      label = "space\nlaser"
    ),
    family = "Bitcount Grid Single Ink",
    size = 30,
    colour = "#DB830B",
    alpha = 1,
    lineheight = 0.7
  ) +
  coord_sf(xlim = c(-1e7, 1e7), ylim = c(-1e7, 1e7)) +
  theme_void() +
  theme(
    aspect.ratio = 1,
    plot.background = element_rect(fill = "#313131ff", colour = NA)
  )

r <- tempfile(fileext = ".png")

ggsave(r, p)


cropcircles::crop_hex(
  r,
  to = "inst/hex/spacelasrerhex.png",
  border_size = 12,
  border_colour = "#74ac90ff"
)
