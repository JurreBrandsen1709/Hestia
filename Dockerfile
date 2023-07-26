# Dockerfile in the root directory for App1

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build-env
WORKDIR /app

# Copy Dyconit and Policies and restore as distinct layers
COPY Dyconit ./Dyconit
COPY Policies ./Policies
WORKDIR /app/Dyconit
RUN dotnet restore

# Build
RUN dotnet publish -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app
COPY --from=build-env /app/Dyconit/out .
COPY --from=build-env /app/Policies ./Policies
ENTRYPOINT ["dotnet", "Dyconit.dll"]
