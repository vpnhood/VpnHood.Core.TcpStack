using Android.Content;
using Android.Graphics;
using Android.Net;
using Activity = Android.App.Activity;

namespace VpnHood.Core.TcpStack.AndroidTest;

[Activity(Label = "@string/app_name", MainLauncher = true )]
public class MainActivity : Activity
{
    private const int VpnRequestCode = 100;
    private TextView _logView = null!;
    private ScrollView _scrollView = null!;

    protected override void OnCreate(Android.OS.Bundle? savedInstanceState)
    {
        base.OnCreate(savedInstanceState);

        var layout = new LinearLayout(this) { Orientation = Orientation.Vertical };
        layout.SetBackgroundColor(Color.Black);

        _logView = new TextView(this) { TextSize = 11 };
        _logView.SetTextColor(Color.LightGray);
        _logView.SetPadding(16, 80, 16, 16);

        _scrollView = new ScrollView(this);
        _scrollView.AddView(_logView);
        layout.AddView(_scrollView, new LinearLayout.LayoutParams(
            LinearLayout.LayoutParams.MatchParent, LinearLayout.LayoutParams.MatchParent));

        SetContentView(layout);

        TestVpnService.OnLog += AppendLog;

        // Request VPN permission on the UI thread; if already granted, launch directly
        var intent = VpnService.Prepare(this);
        if (intent != null)
            StartActivityForResult(intent, VpnRequestCode);
        else
            LaunchService();
    }

    protected override void OnActivityResult(int requestCode, Result resultCode, Intent? data)
    {
        base.OnActivityResult(requestCode, resultCode, data);
        if (requestCode == VpnRequestCode && resultCode == Result.Ok)
            LaunchService();
        else
            AppendLog("VPN permission denied!");
    }

    private void LaunchService()
    {
        StartForegroundService(new Intent(this, typeof(TestVpnService)));
    }

    private void AppendLog(string line)
    {
        RunOnUiThread(() =>
        {
            _logView.Append(line + "\n");
            _scrollView.Post(() => _scrollView.FullScroll(Android.Views.FocusSearchDirection.Down));
        });
    }

    protected override void OnDestroy()
    {
        TestVpnService.OnLog -= AppendLog;
        base.OnDestroy();
    }
}
